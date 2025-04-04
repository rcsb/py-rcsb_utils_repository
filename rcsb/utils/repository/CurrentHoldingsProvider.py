##
#  File:  CurrentHoldingsProvider.py
#  Date:  18-May-2021 jdw
#
#  Updates:
#   12-Jun-2023  dwp Set useCache default to False to force redownloading of holdings files
#    1-Jul-2024  dwp Stop populating "2fo-fc Map" and "fo-fc Map" content types (DSN6 maps), and
#                    only include "Map Coefficients" (MTZ map coefficients) in repository holdings
#    9-Sep-2024  dwp Always defer to loading holdings data from remote (rather than storing it locally);
#                    Add validation coefficients to list of repository_content_types
#   16-Oct-2024  dwp Remove usage of EDMAPS holdings file
#   28-Jan-2025  dwp Add support for IHM holdings file loading
#
##
"""Provide inventory of current repository content.
"""

import datetime
import logging
import os.path

import pytz

from rcsb.utils.io.FileUtil import FileUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil
from rcsb.utils.struct.EntryInfoProvider import EntryInfoProvider

logger = logging.getLogger(__name__)


class CurrentHoldingsProvider(object):
    """Provide inventory of current repository content."""

    def __init__(self, cachePath, useCache=False, **kwargs):
        self.__cachePath = cachePath
        self.__dirPath = os.path.join(cachePath, "holdings")
        self.__storeCache = kwargs.get("storeCache", False)
        self.__repoType = kwargs.get("repoType", "pdb")  # can be set to "pdb" or "pdb_ihm"
        #
        baseUrl = kwargs.get("holdingsTargetUrl", "https://files.wwpdb.org/pub/pdb/holdings")
        fallbackUrl = kwargs.get("holdingsFallbackUrl", "https://files.wwpdb.org/pub/pdb/holdings")
        #
        if self.__repoType == "pdb_ihm":
            baseUrl = baseUrl.replace("pdb/holdings", "pdb_ihm/holdings")
            fallbackUrl = fallbackUrl.replace("pdb/holdings", "pdb_ihm/holdings")
        #
        entryUrlContent = os.path.join(baseUrl, "current_file_holdings.json.gz")
        entryUrlFallbackContent = os.path.join(fallbackUrl, "current_file_holdings.json.gz")
        entryUrlIds = os.path.join(baseUrl, "released_structures_last_modified_dates.json.gz")
        entryUrlFallbackIds = os.path.join(fallbackUrl, "released_structures_last_modified_dates.json.gz")
        #
        self.__mU = MarshalUtil(workPath=self.__dirPath)
        #
        self.__invD = self.__reloadEntryContent(entryUrlContent, entryUrlFallbackContent, self.__dirPath, useCache=useCache)
        self.__idD = self.__reloadEntryIds(entryUrlIds, entryUrlFallbackIds, self.__dirPath, useCache=useCache)
        self.__refD = {}
        #
        if self.__repoType != "pdb_ihm":
            refdataUrlIds = os.path.join(baseUrl, "refdata_id_list.json.gz")
            refdataUrlFallbackIds = os.path.join(fallbackUrl, "refdata_id_list.json.gz")
            self.__refD = self.__reloadRefdataIds(refdataUrlIds, refdataUrlFallbackIds, self.__dirPath, useCache=useCache)
        #
        # EntryInfoProvider must be cached before this class is invoked -
        self.__eiP = EntryInfoProvider(cachePath=self.__cachePath, useCache=True)
        ok = self.__eiP.testCache()
        if not ok:
            self.__eiP = None

    def testCache(self, minCount=220000):
        if self.__repoType == "pdb_ihm":
            minCount = 300
        logger.info("Inventory length cD (%d) id list (%d)", len(self.__invD), len(self.__idD))
        # JDW - restore consistency checks
        # if len(self.__invD) > minCount and len(self.__idD) > minCount and len(self.__invD) == len(self.__idD):
        if len(self.__invD) > minCount and len(self.__idD) > minCount:
            return True
        return False

    def hasEntryContentType(self, entryId, contentType):
        """Return if the current content types is available for the input entry identifier"""
        try:
            return contentType in self.__invD[entryId.upper()]
        except Exception as e:
            logger.exception("Failing for %r with %s", entryId, str(e))
        return False

    def getEntryContentTypes(self, entryId):
        """Return the current content types for the input entry identifier"""
        try:
            return sorted(self.__invD[entryId.upper()].keys())
        except Exception as e:
            logger.exception("Failing for %r with %s", entryId, str(e))
        return []

    def getAllContentTypes(self):
        """Return the all current content types for the repository"""
        entryId = None
        try:
            tS = set()
            for entryId, tD in self.__invD.items():
                for ky in tD:
                    tS.add(ky)
            return sorted(tS)
        except Exception as e:
            logger.exception("Failing for %r with %s", entryId, str(e))
        return []

    def getEntryContentTypePathList(self, entryId, contentType):
        """Return the current content types for the input entry identifier"""
        try:
            return self.__invD[entryId.upper()][contentType]
        except Exception as e:
            logger.debug("Failing for %r %r with %s", entryId, contentType, str(e))
        return []

    def getEntryInventory(self):
        """Return the current inventory dictionary"""
        try:
            return self.__invD
        except Exception as e:
            logger.debug("Failing with %s", str(e))
        return {}

    def getEntryIdList(self, afterDateTimeStamp=None):
        """Return the ID code list or optionally IDs changed after the input time stamp.

        Args:
            afterDateTimeStamp (str, optional): ISO format date time stamp. Defaults to None.
        """
        try:
            if afterDateTimeStamp:
                dt = datetime.datetime.fromisoformat(afterDateTimeStamp).replace(tzinfo=pytz.utc)
                return [k for k, v in self.__idD.items() if v > dt]
            else:
                return list(self.__idD.keys())
        except Exception as e:
            logger.error("Failing with %s", str(e))
        #
        return []

    def getRcsbContentAndAssemblies(self):
        return self.__assembleEntryContentTypes(self.__invD)

    # --- reference data ---
    def getBirdIdList(self):
        return self.__getRefIdListByType("BIRD")

    def getBirdChemCompIdList(self):
        return self.__getRefIdListByType("BIRD CC")

    def getBirdFamilyIdList(self):
        return self.__getRefIdListByType("BIRD Family")

    def getChemCompIdList(self):
        return self.__getRefIdListByType("CC")

    def __getRefIdListByType(self, refType):
        idL = []
        try:
            for rId, tup in self.__refD.items():
                if tup and tup[0] == refType:
                    idL.append(rId)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return idL

    # ---
    def getStatusDetails(self):
        retD = {}
        for entryId in self.__invD:
            retD[entryId] = {"status": "CURRENT", "status_code": "REL"}
        return retD

    def hasValidationReportData(self, entryId):
        return self.__hasValidationReportData(self.__invD, entryId)

    def __hasValidationReportData(self, invD, entryId):
        if entryId.upper() in invD:
            tD = invD[entryId.upper()]
            if "validation_report" in tD:
                for pth in tD["validation_report"]:
                    if pth[-7:] == ".cif.gz":
                        return True
        return False

    def __assembleEntryContentTypes(self, invD):
        # Mapping between repository content types and those used by RCSB.org
        contentTypeD = {
            "assembly_mmcif": "assembly mmCIF",
            "assembly_pdb": "assembly PDB",
            "combined_nmr_data_nef": "Combined NMR data (NEF)",
            "combined_nmr_data_nmr-star": "Combined NMR data (NMR-STAR)",
            "mmcif": "entry mmCIF",
            "nmr_chemical_shifts": "NMR chemical shifts",
            "nmr_restraints_v1": "NMR restraints V1",
            "nmr_restraints_v2": "NMR restraints V2",
            "pdb": "entry PDB",
            "pdb_bundle": "entry PDB bundle",
            "pdbml": "entry PDBML",
            # "pdbml_extatom":  "entry PDBML",
            # "pdbml_noatom": "entry PDBML",
            "structure_factors": "structure factors",
            # Missing mappings
            # "validation report",
            # "validation slider image"
            # "validation 2fo-fc coefficients"
            # "validation fo-fc coefficients"
            # "FASTA sequence",
        }
        #
        noPolymerL = self.__eiP.getEntriesByPolymerEntityCount(count=0) if self.__eiP else []
        logger.info("Entries missing polymers (%d)", len(noPolymerL))
        # for id in noPolymerL:
        #    logger.info("id: %s", id)
        ctD = {}
        assemD = {}
        for entryId, tD in invD.items():
            assemS = set()
            if entryId not in noPolymerL:
                ctD.setdefault(entryId, []).append("FASTA sequence")
            for contentType, pthL in tD.items():
                if contentType in contentTypeD:
                    ctD.setdefault(entryId, []).append(contentTypeD[contentType])
                if contentType == "validation_report":
                    # "/pdb/validation_reports/01/201l/201l_full_validation.pdf.gz"
                    # "/pdb/validation_reports/01/201l/201l_multipercentile_validation.png.gz"
                    # "/pdb/validation_reports/01/201l/201l_multipercentile_validation.svg.gz"
                    # "/pdb/validation_reports/01/201l/201l_validation.pdf.gz"
                    # "/pdb/validation_reports/01/201l/201l_validation.xml.gz"
                    # "/pdb/validation_reports/01/201l/201l_validation_2fo-fc_map_coef.cif.gz"
                    # "/pdb/validation_reports/01/201l/201l_validation_fo-fc_map_coef.cif.gz"
                    for pth in pthL:
                        # Use "_full_validation.pdf.gz" instead of just ".pdf.gz" to avoid re-appending for non-full "_validation.pdf.gz" file
                        if "full_validation.pdf.gz" in pth:
                            ctD.setdefault(entryId, []).append("validation report")
                        elif "validation.svg.gz" in pth:
                            ctD.setdefault(entryId, []).append("validation slider image")
                        elif "validation.cif.gz" in pth:
                            ctD.setdefault(entryId, []).append("validation data mmCIF")
                        elif "validation_2fo-fc_map_coef.cif.gz" in pth:
                            ctD.setdefault(entryId, []).append("validation 2fo-fc coefficients")
                        elif "validation_fo-fc_map_coef.cif.gz" in pth:
                            ctD.setdefault(entryId, []).append("validation fo-fc coefficients")
                if contentType == "assembly_mmcif":
                    # "/pdb/data/biounit/mmCIF/divided/a0/7a09-assembly1.cif.gz"
                    for pth in pthL:
                        fn = os.path.basename(pth)
                        aId = fn.split(".")[0].split("-")[1].replace("assembly", "")
                        assemS.add(aId)

                if contentType == "assembly_pdb":
                    # "/pdb/data/biounit/coordinates/divided/02/302d.pdb1.gz"
                    for pth in pthL:
                        fn = os.path.basename(pth)
                        aId = fn.split(".")[1].replace("pdb", "")
                        assemS.add(aId)
            assemD[entryId] = list(assemS)
        return ctD, assemD

    def __reloadEntryContent(self, urlTarget, urlFallbackTarget, dirPath, useCache=True):
        invD = {}
        fU = FileUtil()
        fn = fU.getFileName(urlTarget)
        if self.__repoType == "pdb_ihm":
            fn = fn.replace(".json", "_ihm.json")  # must do this to prevent overlapping filenames with PDB
        fp = os.path.join(dirPath, fn)
        self.__mU.mkdir(dirPath)
        #
        if self.__storeCache and useCache and self.__mU.exists(fp):
            invD = self.__mU.doImport(fp, fmt="json")
            logger.info("Reading cached inventory (%d)", len(invD))
        else:
            invD = self.__mU.doImport(urlTarget, fmt="json")
            logger.info("Loaded inventory from %s (%r)", urlTarget, len(invD))
            if len(invD) == 0:
                invD = self.__mU.doImport(urlFallbackTarget, fmt="json")
                logger.info("Loaded fallback inventory from %s (%r)", urlFallbackTarget, len(invD))
            #
            # previous method - save file locally
            if self.__storeCache:
                logger.info("Fetch inventory from %s", urlTarget)
                ok = fU.get(urlTarget, fp)
                if not ok:
                    ok = fU.get(urlFallbackTarget, fp)
                if ok:
                    ofp = fp[:-3] if fp.endswith(".gz") else fp
                    ok = self.__mU.doExport(ofp, invD, fmt="json", indent=3)
                    if fp.endswith(".gz"):
                        logger.info("Updating the current entry contents (%r) in %r", ok, fp)
                        fU.compress(ofp, fp)
        return invD

    def __reloadEntryIds(self, urlTarget, urlFallbackTarget, dirPath, useCache=True):
        tD = {}
        idD = {}
        fU = FileUtil()
        fn = fU.getFileName(urlTarget)
        if self.__repoType == "pdb_ihm":
            fn = fn.replace(".json", "_ihm.json")  # must do this to prevent overlapping filenames with PDB
        fp = os.path.join(dirPath, fn)
        self.__mU.mkdir(dirPath)
        #
        if self.__storeCache and useCache and self.__mU.exists(fp):
            tD = self.__mU.doImport(fp, fmt="json")
            logger.info("Reading cached IDs list (%d)", len(tD))
        else:
            tD = self.__mU.doImport(urlTarget, fmt="json")
            logger.info("Loaded ID list from %s (%r)", urlTarget, len(tD))
            if len(tD) == 0:
                tD = self.__mU.doImport(urlFallbackTarget, fmt="json")
                logger.info("Loaded fallback ID list from %s (%r)", urlFallbackTarget, len(tD))
        #
        if self.__storeCache:
            logger.info("Fetch ID list from %s", urlTarget)
            ok = fU.get(urlTarget, fp)
            if not ok:
                ok = fU.get(urlFallbackTarget, fp)
        #
        for k, v in tD.items():
            try:
                idD[k] = datetime.datetime.fromisoformat(v)
            except Exception as e:
                logger.error("Date processing failing for %r %r with %s", k, v, str(e))
        #
        sTupL = sorted(idD.items(), key=lambda item: item[1])
        return {k: v for k, v in sTupL}

    def __reloadRefdataIds(self, urlTarget, urlFallbackTarget, dirPath, useCache=True):
        tD = {}
        idD = {}
        fU = FileUtil()
        fn = fU.getFileName(urlTarget)
        fp = os.path.join(dirPath, fn)
        self.__mU.mkdir(dirPath)
        #
        if self.__storeCache and useCache and self.__mU.exists(fp):
            tD = self.__mU.doImport(fp, fmt="json")
            logger.info("Reading cached IDs list (%d)", len(tD))
        else:
            tD = self.__mU.doImport(urlTarget, fmt="json")
            logger.info("Loaded ID list from %s (%r)", urlTarget, len(tD))
            if len(tD) == 0:
                tD = self.__mU.doImport(urlFallbackTarget, fmt="json")
                logger.info("Loaded fallback ID list from %s (%r)", urlFallbackTarget, len(tD))
        #
        if self.__storeCache:
            logger.info("Fetch ID list from %s", urlTarget)
            ok = fU.get(urlTarget, fp)
            if not ok:
                ok = fU.get(urlFallbackTarget, fp)
        #
        for k, v in tD.items():
            try:
                idD[k] = (v["content_type"], datetime.datetime.fromisoformat(v["last_modified_date"]))
            except Exception as e:
                logger.error("Date processing failing for %r %r with %s", k, v, str(e))
        return idD
