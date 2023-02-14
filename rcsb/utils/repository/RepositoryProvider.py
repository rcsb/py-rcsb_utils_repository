##
# File:    RepositoryProvider.py
# Author:  J. Westbrook
# Date:    21-Mar-2018
#
# Updates:
#   22-Mar-2018  jdw add support for all repositories -
#   26-Mar-2018  jdw internalize the use of externally provided configuration object -
#   27-Mar-2018  jdw add path to support mock repositories for testing.
#   23-May-2018  jdw add getRepoPathList() convenience method
#   18-Jun-2018  jdw move mock support to the configuration module
#   12-Jul-2018  jdw correct config for PDBX_REPO_PATH
#   13-Aug-2018  jdw add support for gz compressed entry files
#   24-Oct-2018  jdw update for new configuration organization
#   28-Nov-2018  jdw add mergeBirdRefData()
#   13-Dec-2018  jdw add preliminary I/HM repository support
#    5-Feb-2019  jdw add just method naming conventions, add getLocator() method,
#                    consolidate deliver of path configuration details in __getRepoTopPath().
#   14-Mar-2019  jdw add VRPT_REPO_PATH_ENV as an override for the validation report repo path.
#   27-Aug-2019  jdw filter missing validation reports
#   16-Sep-2019  jdw consolidate chem_comp_core with bird_chem_comp_core
#   14-Feb-2020  jdw migrate to rcsb.utils.repository
#   17-Sep-2021  jdw add remote repository access methods
#   29-Sep-2021  jdw make default discoveryMode a configuration option add inputIdCodeList to getLocatorObjList()
#    8-Oct-2021  jdw pass configuration URLs to CurrentHoldingsProvider and RemoveHoldingsProvider
#                    ValidationReportProvider() migrated to ValidationReportAdapter()
#    8-Oct-2021  jdw add warning messages for empty read/merge container results in method __mergeContainers()
#    5-Apr-2022  dwp Add support for loading id code lists for bird_chem_comp_core (mainly used for Azure testing)
#   13-Apr-2022  dwp Update methods for obtaining list of computed-model files
#    3-Aug-2022  dwp Enable retrieval of specific computed-model files with input
#    2-Feb-2023  dwp add support for requesting specific inputIdCodeList/idCodeList for CSMs
##
"""
Utilities for scanning and accessing data in PDBx/mmCIF data in common repository file systems or via remote repository services.

"""
__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os
import time

from rcsb.utils.io.HashableDict import HashableDict
from rcsb.utils.io.MarshalUtil import MarshalUtil
from rcsb.utils.io.FileUtil import FileUtil
from rcsb.utils.multiproc.MultiProcUtil import MultiProcUtil
from rcsb.utils.repository.CurrentHoldingsProvider import CurrentHoldingsProvider
from rcsb.utils.repository.RemovedHoldingsProvider import RemovedHoldingsProvider
from rcsb.utils.validation.ValidationReportAdapter import ValidationReportAdapter

logger = logging.getLogger(__name__)


def toCifWrapper(xrt):
    dirPath = os.environ.get("_RP_DICT_PATH_")
    vpr = ValidationReportAdapter(dirPath=dirPath, useCache=True)
    vrd = vpr.getReader()
    return vrd.toCif(xrt)


class RepositoryProvider(object):
    """Utilities for scanning and accessing data in PDBx/mmCIF data in common repository file systems or via remote repository services.

    Discovery modes:

    Local file system mode - data sets are discovered by scanning repository files systems for files matching the require patterns,
                     and repository paths are defined in configuration data. File system path information may be provided
                     explicitly to avoid file system scans. Some merging and consolidation features are provided for validation data
                     and chemical reference data.

    Remote access mode - data sets are discovered from reading repository inventory files,
                     and repository service endpoints are defined in configuration data.


    """

    def __init__(self, cfgOb, cachePath=None, numProc=8, fileLimit=None, verbose=False, discoveryMode=None):
        self.__fileLimit = fileLimit
        self.__numProc = numProc
        self.__verbose = verbose
        self.__cfgOb = cfgOb
        self.__configName = self.__cfgOb.getDefaultSectionName()
        #
        self.__discoveryMode = discoveryMode if discoveryMode else self.__cfgOb.get("DISCOVERY_MODE", sectionName=self.__configName, default="local")
        self.__baseUrlPDB = self.__cfgOb.getPath("PDB_REPO_URL", sectionName=self.__configName, default="https://ftp.wwpdb.org/pub")
        self.__fallbackUrlPDB = self.__cfgOb.getPath("PDB_REPO_FALLBACK_URL", sectionName=self.__configName, default="https://ftp.wwpdb.org/pub")
        self.__baseUrlPDBDev = self.__cfgOb.getPath("PDBDEV_REPO_URL", sectionName=self.__configName, default="https://pdb-dev.wwpdb.org")
        self.__edMapUrl = self.__cfgOb.getPath("RCSB_EDMAP_LIST_PATH", sectionName=self.__configName, default=None)
        #
        self.__kwD = {
            "holdingsTargetUrl": os.path.join(self.__baseUrlPDB, "pdb", "holdings"),
            "holdingsFallbackUrl": os.path.join(self.__fallbackUrlPDB, "pdb", "holdings"),
            "edmapsLocator": self.__edMapUrl,
            "updateTargetUrl": os.path.join(self.__baseUrlPDB, "pdb", "data", "status", "latest"),
            "updateFallbackUrl": os.path.join(self.__fallbackUrlPDB, "pdb", "data", "status", "latest"),
            "filterType": "assign-dates",
        }
        #
        self.__topCachePath = cachePath if cachePath else "."
        self.__cachePath = os.path.join(self.__topCachePath, "repo_util")
        #
        self.__mU = MarshalUtil(workPath=self.__cachePath)
        self.__fU = FileUtil(workPath=self.__cachePath)
        self.__chP = None
        self.__rhP = None
        logger.info("Discovery mode is %r", self.__discoveryMode)
        #

    def getLocatorObjList(self, contentType, inputPathList=None, inputIdCodeList=None, mergeContentTypes=None, excludeIds=None):
        """Convenience method to get the data path list for the input repository content type.

        Args:
            contentType (str): Repository content type (e.g. pdbx, chem_comp, bird, ...)
            inputPathList (list, optional): path list that will be returned if provided (discoveryMode=local).
            inputIdCodeList (list, optional): locators will be returned for this ID code list (discoveryMode=remote).
            mergeContentTypes (list, optional): repository content types to combined with the
                                primary content type.
            excludeIds (list or dict): exclude any locators for idCodes in this list or dictionary

        Returns:
            (list): simple list of data file paths OR a tuple containing file path, format and merge details


        Example:

            locator object ({"locator": <path or URI> "fmt": <supported format code>, 'kwargs': {}},{}, ... )

             supported extensions are:
                 kwargs : {"marshalHelper": <fuction applied post read (e.g. toCifWrapper)>}

             multiple artifacts within the a locator may be optionally merged/consolidated into a single data object.

        """
        inputPathList = inputPathList if inputPathList else []
        inputIdCodeList = inputIdCodeList if inputIdCodeList else []
        if inputPathList:
            return self.__getLocatorObjListWithInput(contentType, inputPathList=inputPathList, mergeContentTypes=mergeContentTypes)
        #
        locatorList = self.__getLocatorList(contentType, inputPathList=inputPathList, inputIdCodeList=inputIdCodeList, mergeContentTypes=mergeContentTypes)
        #
        if excludeIds:
            fL = []
            for locator in locatorList:
                if isinstance(locator, str):
                    pth = locator
                else:
                    pth = locator[0]["locator"]
                #
                idCode = self.__getIdcodeFromLocatorPath(contentType, pth)
                if idCode in excludeIds:
                    continue
                fL.append(locator)
            locatorList = fL

        return locatorList

    def getContainerList(self, locatorObjList):
        """Return the PDBx data container list obtained by parsing the input locator object list."""
        cL = []
        for locatorObj in locatorObjList:
            myContainerList = self.__mergeContainers(locatorObj, fmt="mmcif", mergeTarget=0)
            for cA in myContainerList:
                cL.append(cA)
        return cL

    def getLocatorIdcodes(self, contentType, locatorObjList, locatorIndex=0):
        try:
            if locatorObjList and isinstance(locatorObjList[0], str):
                return [self.__getIdcodeFromLocatorPath(contentType, pth) for pth in locatorObjList]
            else:
                return [self.__getIdcodeFromLocatorPath(contentType, locatorObj[locatorIndex]["locator"]) for locatorObj in locatorObjList]
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return []

    def getLocatorPaths(self, locatorObjList, locatorIndex=0):
        try:
            if locatorObjList and isinstance(locatorObjList[0], str):
                return locatorObjList
            else:
                return [locatorObj[locatorIndex]["locator"] for locatorObj in locatorObjList]
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return []

    def getLocatorsFromPaths(self, locatorObjList, pathList, locatorIndex=0):
        """Return locator objects with paths (locatorObjIndex) matching the input pathList."""
        # index the input locatorObjList
        rL = []
        try:
            if locatorObjList and isinstance(locatorObjList[0], str):
                return pathList
            #
            locIdx = {}
            for ii, locatorObj in enumerate(locatorObjList):
                if "locator" in locatorObj[locatorIndex]:
                    locIdx[locatorObj[locatorIndex]["locator"]] = ii
            #
            for pth in pathList:
                jj = locIdx[pth] if pth in locIdx else None
                if jj is not None:
                    rL.append(locatorObjList[jj])
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        #
        return rL

    #  ---- Private methods ----
    def __getLocatorObjListWithInput(self, contentType, inputPathList=None, mergeContentTypes=None):
        """Convenience method to get the data path list for the input repository content type.
        This is a special case to handle the content merging for the input path/locator list.

        Args:
            contentType (str): Repository content type (e.g. pdbx, chem_comp, bird, ...)
            inputPathList (list, optional): path list that will be returned if provided.
            mergeContentTypes (list, optional): repository content types to combined with the
                                primary content type.

        Returns:
            Obj list: data file paths or tuple of file paths

        """
        inputPathList = inputPathList if inputPathList else []
        locatorList = self.__getLocatorList(contentType, inputPathList=inputPathList)

        if mergeContentTypes and "vrpt" in mergeContentTypes and contentType in ["pdbx", "pdbx_core"]:
            dictPath = os.path.join(self.__topCachePath, self.__cfgOb.get("DICTIONARY_CACHE_DIR", sectionName=self.__cfgOb.getDefaultSectionName()))
            os.environ["_RP_DICT_PATH_"] = dictPath
            #
            locObjL = []
            for locator in locatorList:
                if isinstance(locator, str):
                    kwD = HashableDict({})
                    oL = [HashableDict({"locator": locator, "fmt": "mmcif", "kwargs": kwD})]
                    for mergeContentType in mergeContentTypes:
                        _, fn = os.path.split(locator)
                        idCode = fn[:4] if fn and len(fn) >= 8 else None
                        mergeLocator = self.__getLocator(mergeContentType, idCode, checkExists=True) if idCode else None
                        if mergeLocator:
                            # kwD = HashableDict({"marshalHelper": vrd.toCif})
                            kwD = HashableDict({"marshalHelper": toCifWrapper})
                            oL.append(HashableDict({"locator": mergeLocator, "fmt": "xml", "kwargs": kwD}))
                    lObj = tuple(oL)
                else:
                    logger.error("Unexpected output locator type %r", locator)
                    lObj = locator
                locObjL.append(lObj)
            #
            locatorList = locObjL
        # -
        if contentType in ["pdbx_comp_model_core"]:
            locObjL = []
            for inputPath in inputPathList:
                if isinstance(inputPath, str):
                    if inputPath.strip() in locatorList:
                        locObjL.append(inputPath.strip())
            #
            locatorList = locObjL
        # -
        return locatorList

    def __mergeContainers(self, locatorObj, fmt="mmcif", mergeTarget=0):
        """Consolidate content in auxiliary files locatorObj[1:] into the locatorObj[0] container index 'mergeTarget'."""
        #
        cL = []
        try:
            if isinstance(locatorObj, str):
                cL = self.__mU.doImport(locatorObj, fmt=fmt)
                if not cL:
                    logger.warning("locator %r returns empty container list.", locatorObj)
                return cL if cL else []
            elif isinstance(locatorObj, (list, tuple)) and locatorObj:
                dD = locatorObj[0]
                kw = dD["kwargs"]
                cL = self.__mU.doImport(dD["locator"], fmt=dD["fmt"], **kw)
                if cL:
                    for dD in locatorObj[1:]:
                        kw = dD["kwargs"]
                        rObj = self.__mU.doImport(dD["locator"], fmt=dD["fmt"], **kw)
                        mergeL = rObj if rObj else []
                        for mc in mergeL:
                            cL[mergeTarget].merge(mc)
                else:
                    logger.warning("locator object with leading path %r returns empty container list (%r) ", dD["locator"], locatorObj)
                #
                return cL
            else:
                logger.warning("non-comforming locator object %r", locatorObj)
                return []
        except Exception as e:
            logger.exception("Failing for %r with %s", locatorObj, str(e))

        return cL

    def __getLocatorList(self, contentType, inputPathList=None, inputIdCodeList=None, mergeContentTypes=None):
        if self.__discoveryMode == "local":
            return self.__getLocatorListLocal(contentType, inputPathList=inputPathList, mergeContentTypes=mergeContentTypes)
        else:
            return self.__getLocatorListRemote(contentType, inputIdCodeList=inputIdCodeList, mergeContentTypes=mergeContentTypes)

    def __getLocatorListLocal(self, contentType, inputPathList=None, mergeContentTypes=None):
        """Internal convenience method to return repository local path lists by content type:"""
        outputLocatorList = []
        inputPathList = inputPathList if inputPathList else []
        try:
            if contentType in ["bird", "bird_core"]:
                outputLocatorList = inputPathList if inputPathList else self.__getBirdPathList()
            elif contentType == "bird_family":
                outputLocatorList = inputPathList if inputPathList else self.__getBirdFamilyPathList()
            elif contentType in ["chem_comp"]:
                outputLocatorList = inputPathList if inputPathList else self.__getChemCompPathList()
            elif contentType in ["bird_chem_comp"]:
                outputLocatorList = inputPathList if inputPathList else self.__getBirdChemCompPathList()
            elif contentType in ["pdbx", "pdbx_core"] and mergeContentTypes and "vrpt" in mergeContentTypes:
                dictPath = os.path.join(self.__topCachePath, self.__cfgOb.get("DICTIONARY_CACHE_DIR", sectionName=self.__cfgOb.getDefaultSectionName()))
                os.environ["_RP_DICT_PATH_"] = dictPath
                outputLocatorList = self.__getEntryLocatorObjList(mergeContentTypes=mergeContentTypes)

            elif contentType in ["pdbx", "pdbx_core"]:
                outputLocatorList = inputPathList if inputPathList else self.__getEntryPathList()
            #
            elif contentType in ["pdbx_obsolete"]:
                outputLocatorList = inputPathList if inputPathList else self.getObsoleteEntryPathList()
            elif contentType in ["chem_comp_core", "bird_consolidated", "bird_chem_comp_core"]:
                outputLocatorList = inputPathList if inputPathList else self.mergeBirdAndChemCompRefData()
            elif contentType in ["ihm_dev", "ihm_dev_core", "ihm_dev_full"]:
                outputLocatorList = inputPathList if inputPathList else self.__getIhmDevPathList()
            elif contentType in ["pdb_distro", "da_internal", "status_history"]:
                outputLocatorList = inputPathList if inputPathList else []
            elif contentType in ["pdbx_comp_model_core"]:
                outputLocatorList = inputPathList if inputPathList else self.__getCompModelPathList()
            else:
                logger.warning("Unsupported contentType %s", contentType)
        except Exception as e:
            logger.exception("Failing with %s", str(e))

        if self.__fileLimit:
            outputLocatorList = outputLocatorList[: self.__fileLimit]

        return sorted(outputLocatorList) if outputLocatorList and isinstance(outputLocatorList[0], str) else outputLocatorList

    def __getLocatorListRemote(self, contentType, inputIdCodeList=None, mergeContentTypes=None):
        outputLocatorList = []
        idCodeList = inputIdCodeList if inputIdCodeList else []
        logger.info("Getting remote locator list for contentType %s with idCodeList length (%d)", contentType, len(idCodeList))
        try:
            if contentType in ["bird", "bird_core"]:
                outputLocatorList = self.__getBirdUriList(idCodeList=idCodeList)
            elif contentType == "bird_family":
                outputLocatorList = self.__getBirdFamilyUriList(idCodeList=idCodeList)
            elif contentType in ["chem_comp"]:
                outputLocatorList = self.__getChemCompUriList(idCodeList=idCodeList)
            elif contentType in ["bird_chem_comp"]:
                outputLocatorList = self.__getBirdChemCompUriList(idCodeList=idCodeList)
            elif contentType in ["chem_comp_core", "bird_consolidated", "bird_chem_comp_core"] and not self.__fileLimit:
                outputLocatorList = self.mergeBirdAndChemCompRefData()
            elif contentType in ["chem_comp_core", "bird_consolidated", "bird_chem_comp_core"] and self.__fileLimit:
                outputLocatorList = self.mergeBirdAndChemCompRefDataWithInput(idCodeList=idCodeList)
            #
            elif contentType in ["pdbx", "pdbx_core"]:
                if mergeContentTypes and "vrpt" in mergeContentTypes:
                    dictPath = os.path.join(self.__topCachePath, self.__cfgOb.get("DICTIONARY_CACHE_DIR", sectionName=self.__cfgOb.getDefaultSectionName()))
                    os.environ["_RP_DICT_PATH_"] = dictPath
                    outputLocatorList = self.__getEntryUriList(idCodeList=idCodeList, mergeContentTypes=mergeContentTypes)
                else:
                    outputLocatorList = self.__getEntryUriList(idCodeList=idCodeList)
            #
            elif contentType in ["pdbx_obsolete"]:
                outputLocatorList = self.__getObsoleteEntryUriList(idCodeList=idCodeList)
            #
            elif contentType in ["ihm_dev", "ihm_dev_core", "ihm_dev_full"]:
                outputLocatorList = self.__getIhmDevPathList()
            elif contentType in ["pdbx_comp_model_core"]:
                outputLocatorList = self.__getCompModelPathList(idCodeList=idCodeList)
            else:
                logger.warning("Unsupported contentType %s", contentType)

        except Exception as e:
            logger.exception("Failing with %s", str(e))

        logger.debug("outputLocatorList before applying fileLimit (%r): %r", self.__fileLimit, outputLocatorList)
        if self.__fileLimit:
            outputLocatorList = outputLocatorList[: self.__fileLimit]
            logger.debug("outputLocatorList after applying fileLimit (%r): %r", self.__fileLimit, outputLocatorList)

        return sorted(outputLocatorList) if outputLocatorList and isinstance(outputLocatorList[0], str) else outputLocatorList

    def __getLocator(self, contentType, idCode, version="v1-0", checkExists=False):
        if self.__discoveryMode == "local":
            return self.__getLocatorLocal(contentType, idCode, version=version, checkExists=checkExists)
        else:
            return self.__getLocatorRemote(contentType, idCode)

    def __getLocatorLocal(self, contentType, idCode, version="v1-0", checkExists=False):
        """Convenience method to return repository path for a content type and cardinal identifier."""
        pth = None
        try:
            idCodel = idCode.lower()
            if contentType == "bird":
                pth = os.path.join(self.__getRepoLocalPath(contentType), idCode[-1], idCode + ".cif")
            elif contentType == "bird_family":
                pth = os.path.join(self.__getRepoLocalPath(contentType), idCode[-1], idCode + ".cif")
            elif contentType in ["chem_comp", "chem_comp_core"]:
                pth = os.path.join(self.__getRepoLocalPath(contentType), idCode[0], idCode, idCode + ".cif")
            elif contentType in ["bird_chem_comp"]:
                pth = os.path.join(self.__getRepoLocalPath(contentType), idCode[-1], idCode + ".cif")
            elif contentType in ["pdbx", "pdbx_core", "pdbx_obsolete"]:
                pth = os.path.join(self.__getRepoLocalPath(contentType), idCodel[1:3], idCodel + ".cif.gz")
            elif contentType in ["bird_consolidated", "bird_chem_comp_core"]:
                pth = os.path.join(self.__getRepoLocalPath(contentType), idCode + ".cif")
            elif contentType in ["ihm_dev", "ihm_dev_core", "ihm_dev_full"]:
                pth = os.path.join(self.__getRepoLocalPath(contentType), idCode, idCode + "_model_%s.cif.gz" % version)
            elif contentType in ["pdb_distro", "da_internal", "status_history"]:
                pass
            elif contentType in ["vrpt"]:
                pth = os.path.join(self.__getRepoLocalPath(contentType), idCodel[1:3], idCodel, idCodel + "_validation.xml.gz")
            else:
                logger.warning("Unsupported local contentType %s", contentType)
        except Exception as e:
            logger.exception("Failing with %s", str(e))

        if checkExists:
            pth = pth if self.__mU.exists(pth) else None
        return pth

    def __getLocatorRemote(self, contentType, idCode, repositoryLayout="pdbftp"):
        """Convenience method to return the URI for a content type and cardinal identifier."""
        uri = None
        _ = repositoryLayout
        try:
            idCodel = idCode.lower()
            if contentType == "bird":
                # /pdb/refdata/bird/prd/1/
                uri = os.path.join(self.__baseUrlPDB, "pdb", "refdata", "bird", "prd", idCode[-1], idCode + ".cif")
            elif contentType == "bird_family":
                uri = os.path.join(self.__baseUrlPDB, "pdb", "refdata", "bird", "family", idCode[-1], idCode + ".cif")
            elif contentType in ["bird_chem_comp"]:
                uri = os.path.join(self.__baseUrlPDB, "pdb", "refdata", "bird", "prdcc", idCode[-1], idCode + ".cif")
            elif contentType in ["chem_comp", "chem_comp_core"]:
                uri = os.path.join(self.__baseUrlPDB, "pdb", "refdata", "chem_comp", idCode[-1], idCode, idCode + ".cif")
            #
            elif contentType in ["pdbx", "pdbx_core"]:
                # pdb/data/structures/divided/mmCIF
                uri = os.path.join(self.__baseUrlPDB, "pdb", "data", "structures", "divided", "mmCIF", idCodel[1:3], idCodel + ".cif.gz")
            elif contentType in ["vrpt", "validation_report"]:
                # /pdb/validation_reports/
                # https://ftp.wwpdb.org/pub/pdb/validation_reports/00/100d/100d_validation.xml.gz
                uri = os.path.join(self.__baseUrlPDB, "pdb", "validation_reports", idCodel[1:3], idCodel, idCodel + "_validation.xml.gz")
                # logger.info("uri %r", uri)
            #
            elif contentType in ["pdbx_obsolete"]:
                # pdb/data/structures/obsolete/mmCIF/
                uri = os.path.join(self.__baseUrlPDB, "pdb", "data", "structures", "obsolete", "mmCIF", idCodel[1:3], idCodel + ".cif.gz")
            elif contentType in ["bird_consolidated", "bird_chem_comp_core"]:
                uri = os.path.join(self.__getRepoLocalPath(contentType), idCode + ".cif")
            #
            elif contentType in ["ihm_dev", "ihm_dev_core", "ihm_dev_full"]:

                # https://pdb-dev.wwpdb.org/cif/PDBDEV_00000001.cif
                uri = os.path.join(self.__baseUrlPDBDev, "cif", idCode + ".cif")
            elif contentType in ["pdb_distro", "da_internal", "status_history"]:
                pass
            else:
                logger.warning("Unsupported remote contentType %s", contentType)
        except Exception as e:
            logger.exception("Failing with %s", str(e))

        return uri

    def __getIdcodeFromLocatorPath(self, contentType, pth):
        """Convenience method to return the idcode from the locator path."""
        idCode = None
        try:
            bn = os.path.basename(pth)
            if contentType in ["pdbx", "pdbx_core", "pdbx_obsolete", "bird", "bird_family", "chem_comp", "chem_comp_core", "bird_consolidated", "bird_chem_comp_core"]:
                idCode = bn.split(".")[0]
            elif contentType in ["ihm_dev", "ihm_dev_core", "ihm_dev_full"]:
                tC = bn.split(".")[0]
                idCode = "_".join(tC.split("_")[:2])
            elif contentType in ["pdb_distro", "da_internal", "status_history"]:
                idCode = None
            elif contentType in ["vrpt"]:
                tC = bn.split(".")[0]
                idCode = tC.split("_")[0]
            else:
                logger.warning("Unsupported contentType %s", contentType)
            idCode = idCode.upper() if idCode else None
        except Exception as e:
            logger.exception("Failing for %r %r with %s", contentType, pth, str(e))
        return idCode

    def __getRepoLocalPath(self, contentType):
        """Convenience method to return repository top path from configuration data."""
        pth = None
        try:
            if contentType == "bird":
                pth = self.__cfgOb.getPath("BIRD_REPO_PATH", sectionName=self.__configName)
            elif contentType == "bird_family":
                pth = self.__cfgOb.getPath("BIRD_FAMILY_REPO_PATH", sectionName=self.__configName)
            elif contentType in ["chem_comp", "chem_comp_core"]:
                pth = self.__cfgOb.getPath("CHEM_COMP_REPO_PATH", sectionName=self.__configName)
            elif contentType in ["bird_chem_comp"]:
                pth = self.__cfgOb.getPath("BIRD_CHEM_COMP_REPO_PATH", sectionName=self.__configName)
            elif contentType in ["pdbx", "pdbx_core"]:
                pth = self.__cfgOb.getPath("PDBX_REPO_PATH", sectionName=self.__configName)
            elif contentType in ["pdbx_obsolete"]:
                pth = self.__cfgOb.getPath("PDBX_OBSOLETE_REPO_PATH", sectionName=self.__configName)
            elif contentType in ["pdbx_comp_model_core"]:
                pth = self.__cfgOb.getPath("PDBX_COMP_MODEL_REPO_PATH", sectionName=self.__configName)
            elif contentType in ["bird_consolidated", "bird_chem_comp_core"]:
                pth = self.__cachePath
            elif contentType in ["ihm_dev", "ihm_dev_core", "ihm_dev_full"]:
                pth = self.__cfgOb.getPath("IHM_DEV_REPO_PATH", sectionName=self.__configName)
            elif contentType in ["pdb_distro", "da_internal", "status_history"]:
                pass
            elif contentType in ["vrpt"]:
                pth = self.__cfgOb.getEnvValue("VRPT_REPO_PATH_ENV", sectionName=self.__configName, default=None)
                if pth is None:
                    pth = self.__cfgOb.getPath("VRPT_REPO_PATH", sectionName=self.__configName)
                else:
                    logger.debug("Using validation report path from environment assignment %s", pth)
            else:
                logger.warning("Unsupported contentType %s", contentType)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return pth

    # JDW ---  URI code ----
    def __getEntryUriList(self, idCodeList=None, mergeContentTypes=None):
        uL = []
        try:
            if not self.__chP:
                self.__chP = CurrentHoldingsProvider(self.__topCachePath, useCache=True, **self.__kwD)
            #
            tIdL = self.__chP.getEntryIdList()
            if idCodeList:
                tIdD = dict.fromkeys(tIdL, True)
                tIdL = [idCode.upper() for idCode in idCodeList if idCode.upper() in tIdD]
                # idCodeList = [t.upper() for t in idCodeList]
                # tIdL = list(set(tIdL).intersection(idCodeList))
                logger.debug("idCodeList selected: %r", tIdL)
            #
            for tId in tIdL:
                kwD = HashableDict({})
                locObj = [HashableDict({"locator": self.__getLocatorRemote("pdbx_core", tId), "fmt": "mmcif", "kwargs": kwD})]
                if mergeContentTypes and "vrpt" in mergeContentTypes:
                    # if self.__chP.hasEntryContentType(tId, "validation_report"):
                    if self.__chP.hasValidationReportData(tId):
                        kwD = HashableDict({"marshalHelper": toCifWrapper})
                        locObj.append(HashableDict({"locator": self.__getLocatorRemote("validation_report", tId), "fmt": "xml", "kwargs": kwD}))
                uL.append(tuple(locObj))
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return self.__applyLimit(uL)

    def __getObsoleteEntryUriList(self, idCodeList=None):
        uL = []
        try:
            if not self.__rhP:
                self.__rhP = RemovedHoldingsProvider(self.__topCachePath, useCache=True, **self.__kwD)
            #
            tIdL = self.__rhP.getEntryByStatus("OBS")
            if idCodeList:
                tIdD = dict.fromkeys(tIdL, True)
                tIdL = [idCode.upper() for idCode in idCodeList if idCode.upper() in tIdD]
                logger.info("idCodeList selected: %r", tIdL)
            #
            for tId in tIdL:
                kwD = HashableDict({})
                locObj = [HashableDict({"locator": self.__getLocatorRemote("pdbx_obsolete", tId), "fmt": "mmcif", "kwargs": kwD})]
                uL.append(tuple(locObj))
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return self.__applyLimit(uL)

    def __getBirdUriList(self, idCodeList=None):
        uL = []
        try:
            if not self.__chP:
                self.__chP = CurrentHoldingsProvider(self.__topCachePath, useCache=True, **self.__kwD)
            #
            tIdL = self.__chP.getBirdIdList()
            if idCodeList:
                tIdD = dict.fromkeys(tIdL, True)
                tIdL = [idCode.upper() for idCode in idCodeList if idCode.upper() in tIdD]
                logger.info("idCodeList selected: %r", tIdL)
            #
            kwD = HashableDict({})
            for tId in tIdL:
                uL.append(tuple([HashableDict({"locator": self.__getLocatorRemote("bird", tId), "fmt": "mmcif", "kwargs": kwD})]))
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return self.__applyLimit(uL)

    def __getBirdFamilyUriList(self, idCodeList=None):
        uL = []
        try:
            if not self.__chP:
                self.__chP = CurrentHoldingsProvider(self.__topCachePath, useCache=True, **self.__kwD)
            #
            tIdL = self.__chP.getBirdFamilyIdList()
            if idCodeList:
                tIdD = dict.fromkeys(tIdL, True)
                tIdL = [idCode.upper() for idCode in idCodeList if idCode.upper() in tIdD]
                logger.info("idCodeList selected: %r", tIdL)
            #
            kwD = HashableDict({})
            for tId in tIdL:
                uL.append(tuple([{"locator": self.__getLocatorRemote("bird_family", tId), "fmt": "mmcif", "kwargs": kwD}]))
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return self.__applyLimit(uL)

    def __getChemCompUriList(self, idCodeList=None):
        uL = []
        try:
            if not self.__chP:
                self.__chP = CurrentHoldingsProvider(self.__topCachePath, useCache=True, **self.__kwD)
            #
            tIdL = self.__chP.getChemCompIdList()
            if idCodeList:
                tIdD = dict.fromkeys(tIdL, True)
                tIdL = [idCode.upper() for idCode in idCodeList if idCode.upper() in tIdD]
                logger.info("idCodeList selected: %r", tIdL)
            #
            kwD = HashableDict({})
            for tId in tIdL:
                uL.append(tuple([HashableDict({"locator": self.__getLocatorRemote("chem_comp", tId), "fmt": "mmcif", "kwargs": kwD})]))
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return self.__applyLimit(uL)

    def __getBirdChemCompUriList(self, idCodeList=None):
        uL = []
        try:
            if not self.__chP:
                self.__chP = CurrentHoldingsProvider(self.__topCachePath, useCache=True, **self.__kwD)
            #
            tIdL = self.__chP.getBirdChemCompIdList()
            if idCodeList:
                tIdD = dict.fromkeys(tIdL, True)
                tIdL = [idCode.upper() for idCode in idCodeList if idCode.upper() in tIdD]
                logger.info("idCodeList selected: %r", tIdL)
            #
            kwD = HashableDict({})
            for tId in tIdL:
                uL.append(tuple([HashableDict({"locator": self.__getLocatorRemote("bird_chem_comp", tId), "fmt": "mmcif", "kwargs": kwD})]))
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return self.__applyLimit(uL)

    # -- Path based code ---
    def _chemCompPathWorker(self, dataList, procName, optionsD, workingDir):
        """Return the list of chemical component definition file paths in the current repository."""
        _ = procName
        _ = workingDir
        topRepoPath = optionsD["topRepoPath"]
        pathList = []
        for subdir in dataList:
            dd = os.path.join(topRepoPath, subdir)
            for root, _, files in os.walk(dd, topdown=False):
                if "REMOVE" in root:
                    continue
                for name in files:
                    if name.endswith(".cif") and len(name) <= 7:
                        pathList.append(os.path.join(root, name))
        return dataList, pathList, []

    def __getChemCompPathList(self):
        return self.__fetchChemCompPathList(self.__getRepoLocalPath("chem_comp"), numProc=self.__numProc)

    def __fetchChemCompPathList(self, topRepoPath, numProc=8):
        """Get the path list for the chemical component definition repository"""
        ts = time.strftime("%Y %m %d %H:%M:%S", time.localtime())
        logger.debug("Starting at %s", ts)
        startTime = time.time()
        pathList = []
        try:
            dataS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
            dataList = [a for a in dataS]
            optD = {}
            optD["topRepoPath"] = topRepoPath
            mpu = MultiProcUtil(verbose=self.__verbose)
            mpu.setOptions(optionsD=optD)
            mpu.set(workerObj=self, workerMethod="_chemCompPathWorker")
            _, _, retLists, _ = mpu.runMulti(dataList=dataList, numProc=numProc, numResults=1)
            pathList = retLists[0]
            endTime0 = time.time()
            logger.debug("Path list length %d  in %.4f seconds", len(pathList), endTime0 - startTime)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return self.__applyLimit(pathList)

    def _entryLocatorObjWithMergeWorker(self, dataList, procName, optionsD, workingDir):
        """Return the list of entry locator objects including merge content in the current repository."""
        _ = procName
        _ = workingDir
        topRepoPath = optionsD["topRepoPath"]
        mergeContentTypes = optionsD["mergeContentTypes"]
        locatorObjList = []
        for subdir in dataList:
            dd = os.path.join(topRepoPath, subdir)
            for root, _, files in os.walk(dd, topdown=False):
                if "REMOVE" in root:
                    continue
                for fn in files:
                    if (fn.endswith(".cif.gz") and len(fn) == 11) or (fn.endswith(".cif") and len(fn) == 8):
                        locator = os.path.join(root, fn)
                        kwD = HashableDict({})
                        oL = [HashableDict({"locator": locator, "fmt": "mmcif", "kwargs": kwD})]
                        for mergeContentType in mergeContentTypes:
                            idCode = fn[:4] if fn and len(fn) >= 8 else None
                            mergeLocator = self.__getLocator(mergeContentType, idCode, checkExists=True) if idCode else None
                            if mergeLocator:
                                kwD = HashableDict({"marshalHelper": toCifWrapper})
                                oL.append(HashableDict({"locator": mergeLocator, "fmt": "xml", "kwargs": kwD}))
                        lObj = tuple(oL)
                        locatorObjList.append(lObj)
        return dataList, locatorObjList, []

    def __getEntryLocatorObjList(self, mergeContentTypes=None):
        return self.__fetchEntryLocatorObjList(self.__getRepoLocalPath("pdbx"), numProc=self.__numProc, mergeContentTypes=mergeContentTypes)

    def __fetchEntryLocatorObjList(self, topRepoPath, numProc=8, mergeContentTypes=None):
        """Get the path list for structure entries in the input repository"""
        ts = time.strftime("%Y %m %d %H:%M:%S", time.localtime())
        logger.debug("Starting at %s", ts)
        startTime = time.time()
        pathList = []
        try:
            dataList = []
            anL = "abcdefghijklmnopqrstuvwxyz0123456789"
            for a1 in anL:
                for a2 in anL:
                    hc = a1 + a2
                    dataList.append(hc)
                    hc = a2 + a1
                    dataList.append(hc)
            dataList = list(set(dataList))
            #
            optD = {}
            optD["topRepoPath"] = topRepoPath
            optD["mergeContentTypes"] = mergeContentTypes
            mpu = MultiProcUtil(verbose=self.__verbose)
            mpu.setOptions(optionsD=optD)
            mpu.set(workerObj=self, workerMethod="_entryLocatorObjWithMergeWorker")
            _, _, retLists, _ = mpu.runMulti(dataList=dataList, numProc=numProc, numResults=1)
            pathList = retLists[0]
            endTime0 = time.time()
            logger.debug("Locator object list length %d  in %.4f seconds", len(pathList), endTime0 - startTime)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return self.__applyLimit(pathList)

    def _entryPathWorker(self, dataList, procName, optionsD, workingDir):
        """Return the list of entry file paths in the current repository."""
        _ = procName
        _ = workingDir
        topRepoPath = optionsD["topRepoPath"]
        pathList = []
        for subdir in dataList:
            dd = os.path.join(topRepoPath, subdir)
            for root, _, files in os.walk(dd, topdown=False):
                if "REMOVE" in root:
                    continue
                for name in files:
                    if (name.endswith(".cif.gz") and len(name) == 11) or (name.endswith(".cif") and len(name) == 8):
                        pathList.append(os.path.join(root, name))
        return dataList, pathList, []

    def _compModelPathWorker(self, dataList, procName, optionsD, workingDir):
        """Return the list of computed-model entry file paths in the current computed-model storage area.

        Args:
            dataList (list): List of sub-directory paths to model files in the computed-model storage area, with paths starting at
                             the model source prefix (e.g., ["AF/XJ/E6/AF_AFA0A385XJE6F1.cif.gz", "MA/PC/05/MA_MABAKCEPC0534.cif.gz."])
            procName (str): worker process name
            optionsD (dict): dictionary of additional options that worker can access
            workingDir (str): path to working directory
        """
        _ = procName
        _ = workingDir
        topRepoPath = optionsD["topRepoPath"]
        pathList = []
        for modelPath in dataList:
            pathList.append(os.path.join(topRepoPath, modelPath))
        return dataList, pathList, []

    def __getEntryPathList(self):
        return self.__fetchEntryPathList(self.__getRepoLocalPath("pdbx"), numProc=self.__numProc)

    def getObsoleteEntryPathList(self):
        return self.__fetchEntryPathList(self.__getRepoLocalPath("pdbx_obsolete"), numProc=self.__numProc)

    def __fetchEntryPathList(self, topRepoPath, numProc=8):
        """Get the path list for structure entries in the input repository"""
        ts = time.strftime("%Y %m %d %H:%M:%S", time.localtime())
        logger.debug("Starting at %s", ts)
        startTime = time.time()
        pathList = []
        try:
            dataList = []
            anL = "abcdefghijklmnopqrstuvwxyz0123456789"
            for a1 in anL:
                for a2 in anL:
                    hc = a1 + a2
                    dataList.append(hc)
                    hc = a2 + a1
                    dataList.append(hc)
            dataList = list(set(dataList))
            #
            optD = {}
            optD["topRepoPath"] = topRepoPath
            mpu = MultiProcUtil(verbose=self.__verbose)
            mpu.setOptions(optionsD=optD)
            mpu.set(workerObj=self, workerMethod="_entryPathWorker")
            _, _, retLists, _ = mpu.runMulti(dataList=dataList, numProc=numProc, numResults=1)
            pathList = retLists[0]
            endTime0 = time.time()
            logger.debug("Path list length %d  in %.4f seconds", len(pathList), endTime0 - startTime)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return self.__applyLimit(pathList)

    def __getBirdPathList(self):
        return self.__fetchBirdPathList(self.__getRepoLocalPath("bird"))

    def __fetchBirdPathList(self, topRepoPath):
        """Return the list of definition file paths in the current repository.

        List is ordered in increasing PRD ID numerical code.
        """
        pathList = []
        try:
            sd = {}
            for root, _, files in os.walk(topRepoPath, topdown=False):
                if "REMOVE" in root:
                    continue
                for name in files:
                    if name.startswith("PRD_") and name.endswith(".cif") and len(name) <= 14:
                        pth = os.path.join(root, name)
                        sd[int(name[4:-4])] = pth
            #
            for k in sorted(sd.keys()):
                pathList.append(sd[k])
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        #
        return self.__applyLimit(pathList)

    def __getBirdFamilyPathList(self):
        return self.__fetchBirdFamilyPathList(self.__getRepoLocalPath("bird_family"))

    def __fetchBirdFamilyPathList(self, topRepoPath):
        """Return the list of definition file paths in the current repository.

        List is ordered in increasing PRD ID numerical code.
        """
        pathList = []
        try:
            sd = {}
            for root, _, files in os.walk(topRepoPath, topdown=False):
                if "REMOVE" in root:
                    continue
                for name in files:
                    if name.startswith("FAM_") and name.endswith(".cif") and len(name) <= 14:
                        pth = os.path.join(root, name)
                        sd[int(name[4:-4])] = pth
            #
            for k in sorted(sd.keys()):
                pathList.append(sd[k])
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        #
        return self.__applyLimit(pathList)

    def __getBirdChemCompPathList(self):
        return self.__fetchBirdChemCompPathList(self.__getRepoLocalPath("bird_chem_comp"))

    def __fetchBirdChemCompPathList(self, topRepoPath):
        """Return the list of definition file paths in the current repository.

        List is ordered in increasing PRD ID numerical code.
        """
        pathList = []
        try:
            sd = {}
            for root, _, files in os.walk(topRepoPath, topdown=False):
                if "REMOVE" in root:
                    continue
                for name in files:
                    if name.startswith("PRDCC_") and name.endswith(".cif") and len(name) <= 16:
                        pth = os.path.join(root, name)
                        sd[int(name[6:-4])] = pth
            #
            for k in sorted(sd.keys()):
                pathList.append(sd[k])
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        #
        return self.__applyLimit(pathList)

    def __applyLimit(self, itemList):
        logger.debug("Length of item list %d (limit %r)", len(itemList), self.__fileLimit)
        if self.__fileLimit:
            return itemList[: self.__fileLimit]
        else:
            return itemList

    def __buildFamilyIndex(self):
        """Using information from the PRD family definition:
        #
        loop_
        _pdbx_reference_molecule_list.family_prd_id
        _pdbx_reference_molecule_list.prd_id
            FAM_000010 PRD_000041
            FAM_000010 PRD_000042
            FAM_000010 PRD_000043
            FAM_000010 PRD_000044
            FAM_000010 PRD_000048
            FAM_000010 PRD_000049
            FAM_000010 PRD_000051
        #
        """
        prdD = {}
        try:
            pthL = self.getLocatorPaths(self.__getLocatorList("bird_family"))
            for pth in pthL:
                containerL = self.__mU.doImport(pth, fmt="mmcif")
                for container in containerL:
                    catName = "pdbx_reference_molecule_list"
                    if container.exists(catName):
                        catObj = container.getObj(catName)
                        for ii in range(catObj.getRowCount()):
                            familyPrdId = catObj.getValue(attributeName="family_prd_id", rowIndex=ii)
                            prdId = catObj.getValue(attributeName="prd_id", rowIndex=ii)
                            if prdId in prdD:
                                logger.debug("duplicate prdId in family index %s %s", prdId, familyPrdId)
                            prdD[prdId] = {"familyPrdId": familyPrdId, "c": container}
        except Exception as e:
            logger.exception("Failing with %s", str(e))

        return prdD

    def __buildBirdCcIndex(self, idCodeList=None):
        """Using information from the PRD pdbx_reference_molecule category to
        index the BIRDs corresponding small molecule correspondences

        """
        prdD = {}
        ccPathD = {}
        prdStatusD = {}
        try:
            ccPathL = self.getLocatorPaths(self.__getLocatorList("chem_comp", inputIdCodeList=idCodeList))
            logger.debug("ccPathL: %r", ccPathL)
            ccPathD = {}
            for ccPath in ccPathL:
                _, fn = os.path.split(ccPath)
                ccId, _ = os.path.splitext(fn)
                ccPathD[ccId] = ccPath
            logger.info("Chemical component path list (%d)", len(ccPathD))
            # logger.info("Chemical component path list: %r", ccPathD)
            #
            pthL = self.getLocatorPaths(self.__getLocatorList("bird", inputIdCodeList=idCodeList))
            logger.info("BIRD path list (%d)", len(pthL))
            # logger.info("BIRD path list: %r", pthL)
            #
            for pth in pthL:
                containerL = self.__mU.doImport(pth, fmt="mmcif")
                for container in containerL:
                    catName = "pdbx_reference_molecule"
                    if container.exists(catName):
                        catObj = container.getObj(catName)
                        ii = 0
                        prdId = catObj.getValue(attributeName="prd_id", rowIndex=ii)
                        relStatus = catObj.getValue(attributeName="release_status", rowIndex=ii)
                        prdStatusD[prdId] = relStatus
                        if relStatus != "REL":
                            continue
                        prdRepType = catObj.getValue(attributeName="represent_as", rowIndex=ii)
                        logger.debug("represent as %r", prdRepType)
                        if prdRepType in ["single molecule"]:
                            ccId = catObj.getValueOrDefault(attributeName="chem_comp_id", rowIndex=ii, defaultValue=None)
                            # prdId = catObj.getValue(attributeName="prd_id", rowIndex=ii)
                            logger.debug("mapping prdId %r ccId %r", prdId, ccId)
                            if ccId and ccId in ccPathD:
                                prdD[prdId] = {"ccId": ccId, "ccPath": ccPathD[ccId]}
                                ccPathD[ccPathD[ccId]] = {"ccId": ccId, "prdId": prdId}
                            else:
                                logger.warning("Missing ccId %r referenced in BIRD %r", ccId, prdId)

        except Exception as e:
            logger.exception("Failing with %s", str(e))
        logger.info("Candidate Chemical Components (%d) BIRDS (%d) BIRD status details (%d)", len(prdD), len(ccPathD), len(prdStatusD))
        return prdD, ccPathD, prdStatusD

    # -
    def mergeBirdAndChemCompRefData(self):
        # JDW note that this merging procedure expects access to all reference data -
        # Use this method when:  self.__fileLimit = None
        prdSmallMolCcD, ccPathD, prdStatusD = self.__buildBirdCcIndex()
        logger.info("PRD to CCD index length %d CCD map path length %d", len(prdSmallMolCcD), len(ccPathD))
        outputPathList = self.__mergeBirdRefData(prdSmallMolCcD, prdStatusD)
        #
        ccOutputPathList = []
        if self.__discoveryMode == 'remote':
            for pth in self.__getChemCompUriList(idCodeList=None):
                ccp = pth[0]['locator']
                if ccp not in ccPathD:
                    ccOutputPathList.append(ccp)
        else:
            ccOutputPathList = [pth for pth in self.__getChemCompPathList() if pth not in ccPathD]
        #
        outputPathList.extend(ccOutputPathList)
        logger.info("Total cc paths: %d", len(ccOutputPathList))
        logger.info("Total bird_chem_comp paths: %d", len(outputPathList))
        #
        return outputPathList

    def mergeBirdAndChemCompRefDataWithInput(self, idCodeList=None):
        # Use this method when:  self.__fileLimit != None
        idCodeList = idCodeList if idCodeList else []
        prdSmallMolCcD, ccPathD, prdStatusD = self.__buildBirdCcIndex(idCodeList=idCodeList)
        logger.info("PRD to CCD index length %d CCD map path length %d", len(prdSmallMolCcD), len(ccPathD))
        outputPathList = self.__mergeBirdRefData(prdSmallMolCcD, prdStatusD, idCodeList=idCodeList)
        #
        ccOutputPathList = []
        if self.__discoveryMode == 'remote':
            for pth in self.__getChemCompUriList(idCodeList=idCodeList):
                ccp = pth[0]['locator']
                if ccp not in ccPathD:
                    ccOutputPathList.append(ccp)
        else:
            ccOutputPathList = [pth for pth in self.__getChemCompPathList() if pth not in ccPathD]
        #
        outputPathList.extend(ccOutputPathList)
        logger.info("Total cc paths: %d", len(ccOutputPathList))
        logger.info("Total bird_chem_comp paths: %d", len(outputPathList))
        #
        return outputPathList

    def __mergeBirdRefData(self, prdSmallMolCcD, prdStatusD, idCodeList=None):
        """Consolidate all of the bird reference data in a single container.

        If the BIRD is a 'small molecule' type then also merge with the associated CC definition.

        Store the merged data in the REPO_UTIL cache path and ...

        Return a path list for the consolidated data files -

        """
        outPathList = []
        iSkipUnreleased = 0
        try:
            birdPathList = self.getLocatorPaths(self.__getLocatorList("bird", inputIdCodeList=idCodeList))
            birdPathD = {}
            for birdPath in birdPathList:
                _, fn = os.path.split(birdPath)
                prdId, _ = os.path.splitext(fn)
                birdPathD[prdId] = birdPath
            #
            logger.info("BIRD path length %d", len(birdPathD))
            logger.debug("BIRD keys %r", list(birdPathD.keys()))
            birdCcPathList = self.getLocatorPaths(self.__getLocatorList("bird_chem_comp", inputIdCodeList=idCodeList))
            birdCcPathD = {}
            for birdCcPath in birdCcPathList:
                _, fn = os.path.split(birdCcPath)
                prdCcId, _ = os.path.splitext(fn)
                prdId = "PRD_" + prdCcId[6:]
                birdCcPathD[prdId] = birdCcPath
            #
            logger.info("BIRDCC path length %d", len(birdCcPathD))
            logger.debug("BIRD CC keys %r", list(birdCcPathD.keys()))
            fD = self.__buildFamilyIndex()
            logger.info("BIRD Family index length %d", len(fD))
            logger.debug("Family index keys %r", list(fD.keys()))
            logger.info("PRD to CCD small mol index length %d", len(prdSmallMolCcD))
            #
            for prdId in birdPathD:
                if prdId in prdStatusD and prdStatusD[prdId] != "REL":
                    logger.debug("Skipping BIRD with non-REL status %s", prdId)
                    iSkipUnreleased += 1
                    continue
                fp = os.path.join(self.__cachePath, prdId + ".cif")
                logger.debug("Export cache path is %r", fp)
                #
                pth2 = birdPathD[prdId]
                cL = self.__mU.doImport(pth2, fmt="mmcif")
                cFull = cL[0]
                logger.debug("Got Bird %r", cFull.getName())
                #
                # --- JDW
                # add missing one_letter_codes item
                if cFull.exists("pdbx_reference_entity_sequence") and cFull.exists("pdbx_reference_entity_poly_seq"):
                    aaDict3 = {
                        "ALA": "A",
                        "ARG": "R",
                        "ASN": "N",
                        "ASP": "D",
                        "ASX": "B",
                        "CYS": "C",
                        "GLN": "Q",
                        "GLU": "E",
                        "GLX": "Z",
                        "GLY": "G",
                        "HIS": "H",
                        "ILE": "I",
                        "LEU": "L",
                        "LYS": "K",
                        "MET": "M",
                        "PHE": "F",
                        "PRO": "P",
                        "SER": "S",
                        "THR": "T",
                        "TRP": "W",
                        "TYR": "Y",
                        "VAL": "V",
                        "PYL": "O",
                        "SEC": "U",
                    }
                    catObj = cFull.getObj("pdbx_reference_entity_sequence")
                    if not catObj.hasAttribute("one_letter_codes"):
                        logger.debug("adding one letter codes for %r", prdId)
                        seqObj = cFull.getObj("pdbx_reference_entity_poly_seq")
                        seqD = {}
                        for jj in range(0, seqObj.getRowCount()):
                            entityId = seqObj.getValue("ref_entity_id", jj)
                            monId = seqObj.getValue("mon_id", jj)
                            seqD.setdefault(entityId, []).append(monId)
                        #
                        logger.debug("seqD %r", seqD)
                        catObj.appendAttribute("one_letter_codes")
                        for ii in range(catObj.getRowCount()):
                            entityId = catObj.getValue("ref_entity_id", ii)
                            if entityId in seqD:
                                ttL = [aaDict3[tt] if tt in aaDict3 else "X" for tt in seqD[entityId]]
                                catObj.setValue("".join(ttL), "one_letter_codes", ii)
                            else:
                                logger.error("%r missing sequence for entity %r", prdId, entityId)
                # ---
                #
                ccBird = None
                ccD = None
                if prdId in prdSmallMolCcD:
                    try:
                        pthCc = prdSmallMolCcD[prdId]["ccPath"]
                        cL = self.__mU.doImport(pthCc, fmt="mmcif")
                        ccD = cL[0]
                    except Exception as e:
                        logger.error("(%s) failed getting path %r: %r", prdId, pthCc, str(e))
                    #
                elif prdId in birdCcPathD:
                    try:
                        pth1 = birdCcPathD[prdId]
                        c1L = self.__mU.doImport(pth1, fmt="mmcif")
                        ccBird = c1L[0]
                    except Exception as e:
                        logger.error("(%s) Failed getting path %r: %r", prdId, pth1, str(e))
                    #
                cFam = None
                if prdId in fD:
                    cFam = fD[prdId]["c"]
                    logger.debug("Got cFam %r", cFam.getName())
                #
                if ccD:
                    for catName in ccD.getObjNameList():
                        cFull.append(ccD.getObj(catName))
                #
                if ccBird:
                    for catName in ccBird.getObjNameList():
                        cFull.append(ccBird.getObj(catName))
                if cFam:
                    for catName in cFam.getObjNameList():
                        cFull.append(cFam.getObj(catName))
                #
                self.__mU.doExport(fp, [cFull], fmt="mmcif")
                outPathList.append(fp)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        #
        logger.info("Merged BIRD/Family/CC path length %d (skipped non-released %d)", len(outPathList), iSkipUnreleased)
        return outPathList
        #

    def __getCompModelPathList(self, idCodeList=None):
        return self.__fetchModelPathList(self.__getRepoLocalPath("pdbx_comp_model_core"), idCodeList=idCodeList, numProc=self.__numProc)

    def __fetchModelPathList(self, topRepoPath, idCodeList=None, numProc=8):
        """Get the path list for computational models in the input cached model repository

        TO-DO: Add check of cache file to see if it changed between the last time data was uploaded, and if so, then upload new models

        File name template is:  <topRepoPath>/<2-char source>/<hash>/<hash>/*.cif.gz
        """
        ts = time.strftime("%Y %m %d %H:%M:%S", time.localtime())
        logger.info("Starting at %s", ts)
        logger.info("Computed-models topRepoPath: %s", topRepoPath)
        startTime = time.time()
        #
        idCodeList = idCodeList if idCodeList else []
        pathList = []
        try:
            compModelCacheFile, cacheFmt, compressed = self.__getCompModelCachPath()
            if not compModelCacheFile:
                logger.info("Failed to determine path of computed-models cache file. Returning empty pathList.")
                return pathList
            if cacheFmt == "pickle" and compressed:
                compModelCacheFile = self.__fU.uncompress(compModelCacheFile)
            compModelCacheD = self.__mU.doImport(compModelCacheFile, fmt=cacheFmt)
            #
            dataList = []
            if len(idCodeList) > 0:
                for mId in idCodeList:
                    dataList.append(compModelCacheD[mId]["modelPath"])
            else:
                for _, modelD in compModelCacheD.items():
                    dataList.append(modelD["modelPath"])
            logger.info("Computed-models loaded dataList length: %d", len(dataList))
            #
            optD = {}
            optD["topRepoPath"] = topRepoPath
            mpu = MultiProcUtil(verbose=self.__verbose)
            mpu.setOptions(optionsD=optD)
            mpu.set(workerObj=self, workerMethod="_compModelPathWorker")
            _, _, retLists, _ = mpu.runMulti(dataList=dataList, numProc=numProc, numResults=1)
            pathList = retLists[0]
            endTime0 = time.time()
            logger.debug("Path list length %d  in %.4f seconds", len(pathList), endTime0 - startTime)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return self.__applyLimit(pathList)

    def __getCompModelCachPath(self):
        """Convenience method to return path for computed-model cache file (json or pickle),
        which contains the list of all computed-models in storage area.
        """
        pth = None
        fmt = None
        compressed = False
        try:
            pth = self.__cfgOb.getPath("PDBX_COMP_MODEL_CACHE_LIST_PATH", sectionName=self.__configName)
            if pth.endswith(".pic") or pth.endswith(".pic.gz"):
                fmt = "pickle"
            elif pth.endswith(".json") or pth.endswith(".json.gz"):
                fmt = "json"
            else:
                logger.warning("Unsupported format/extension for computed-model cache file %s", pth)
            if pth.endswith(".gz"):
                compressed = True
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return pth, fmt, compressed

    def getCompModelIdMap(self):
        return self.__fetchCompModelIdMap()

    def __fetchCompModelIdMap(self):
        """Get the ID mapping between the source model IDs and internal model identifiers for computational models.
        """
        #
        compModelIdMapD = {}
        try:
            compModelCacheFile, cacheFmt, compressed = self.__getCompModelCachPath()
            if not compModelCacheFile:
                logger.info("Failed to determine path of computed-models cache file. Returning empty compModelIdMapD.")
                return compModelIdMapD
            if cacheFmt == "pickle" and compressed:
                compModelCacheFile = self.__fU.uncompress(compModelCacheFile)
            compModelCacheD = self.__mU.doImport(compModelCacheFile, fmt=cacheFmt)
            for internalModelId, modelD in compModelCacheD.items():
                compModelIdMapD.update({modelD["sourceId"]: internalModelId})
            logger.info("Computed-models mapped ID length: %d", len(compModelIdMapD))
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return compModelIdMapD

    def __getIhmDevPathList(self):
        return self.__fetchIhmDevPathList(self.__getRepoLocalPath("ihm_dev"))

    def __fetchIhmDevPathList(self, topRepoPath):
        """Return the list of I/HM entries in the current repository.

        File name template is: PDBDEV_0000 0020_model_v1-0.cif.gz

        List is ordered in increasing PRDDEV numerical code.
        """
        pathList = []
        logger.debug("Searching path %r", topRepoPath)
        try:
            sd = {}
            for root, _, files in os.walk(topRepoPath, topdown=False):
                if "REMOVE" in root:
                    continue
                for name in files:
                    if name.startswith("PDBDEV_") and name.endswith(".cif.gz") and len(name) <= 50:
                        pth = os.path.join(root, name)
                        sd[int(name[7:15])] = pth
            #
            for k in sorted(sd.keys()):
                pathList.append(sd[k])
        except Exception as e:
            logger.exception("Failing search in %r with %s", topRepoPath, str(e))
        #
        return self.__applyLimit(pathList)
