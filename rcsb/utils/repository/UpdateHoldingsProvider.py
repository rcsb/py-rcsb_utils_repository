##
#  File:  UpdateHoldingsProvider.py
#  Date:  18-Sep-2021 jdw
#
#  Updates:
#
##
"""Provide inventory of current repository update.
"""

import collections
import logging
import os.path

from rcsb.utils.io.MarshalUtil import MarshalUtil

logger = logging.getLogger(__name__)


class UpdateHoldingsProvider(object):
    """Provide inventory of the current repository update."""

    def __init__(self, cachePath, useCache, **kwargs):
        self.__dirPath = os.path.join(cachePath, "holdings")
        #
        baseUrl = kwargs.get("updateTargetUrl", "https://ftp.wwpdb.org/pub/pdb/data/status/latest")
        fallbackUrl = kwargs.get("updateFallbackUrl", "https://ftp.wwpdb.org/pub/pdb/data/status/latest")
        #
        self.__mU = MarshalUtil(workPath=self.__dirPath)
        self.__updD = self.__reloadUpdateLists(baseUrl, fallbackUrl, self.__dirPath, useCache=useCache)

    def testCache(self, minCount=100):
        logger.info("Length update updD (%d)", len(self.__updD) if self.__updD else 0)
        if self.__updD and len(self.__updD) > minCount:
            return True
        return False

    def getUpdateData(self):
        return self.__updD

    def __reloadUpdateLists(self, urlTarget, urlFallbackTarget, dirPath, useCache=True):
        """Parse legacy lists defining the contents of the repository update

        Args:
            urlTarget (str): base url for ftp repository instance
            urlFallbackTarget (str): fallback base url for ftp repository instance
            dirPath (str): cache directory path containing update list files
            **kwargs: unused

        Returns:
            list: List of dictionaries containing rcsb_repository_holdings_update

        """
        _ = urlFallbackTarget
        contentTypeList = ["pdb", "nmr", "cs", "sf", "nmrdata"]
        contentNameD = {
            "pdb": "coordinates",
            "nmr": "NMR restraints",
            "cs": "NMR chemical shifts",
            "sf": "structure factors",
            "nmrdata": "Combined NMR data (NEF)"
            # "nmrdata": "Combined NMR data (NMR-STAR)",
        }
        retD = {}
        fp = os.path.join(dirPath, "update_holdings.json")
        if useCache and self.__mU.exists(fp):
            retD = self.__mU.doImport(fp, fmt="json")
            logger.debug("Reading update cached IDs  (%d)", len(retD))
        else:
            try:
                updateTypeList = ["added", "modified", "obsolete"]
                #
                for updateType in updateTypeList:
                    uD = {}
                    for contentType in contentTypeList:
                        fp1 = os.path.join(urlTarget, updateType + "." + contentType)
                        if not self.__mU.exists(fp1):
                            logger.debug("skipping missing resource %r", fp1)
                            continue
                        entryIdL = self.__mU.doImport(fp1, "list")
                        #
                        for entryId in entryIdL:
                            entryId = entryId.strip().upper()
                            uD.setdefault(entryId, []).append(contentNameD[contentType])
                            if contentType == "nmrdata":
                                uD.setdefault(entryId, []).append("Combined NMR data (NMR-STAR)")
                    for entryId in uD:
                        uType = "removed" if updateType == "obsolete" else updateType
                        if entryId in retD:
                            logger.warning("Redundant entry in update list (%r) %r", updateType, entryId)
                        retD[entryId] = {"update_type": uType, "repository_content_types": uD[entryId]}
                ok = self.__mU.doExport(fp, retD, fmt="json", indent=3)
                logger.info("Stored update holdings (%r) for %d entries in %r", ok, len(retD), fp)

                tD = collections.Counter([v["update_type"] for v in retD.values()])
                logger.info("Update counts %r", tD)

            except Exception as e:
                logger.exception("Failing with %s", str(e))
        return retD
