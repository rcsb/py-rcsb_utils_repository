##
#  File:  UnreleasedHoldingsProvider.py
#  Date:  24-Sep-2021 jdw
#
#  Updates:
#   12-Jun-2023  dwp Set useCache default to False to force redownloading of holdings files
#    9-Sep-2024  dwp Always defer to loading holdings data from remote (rather than storing it locally)
#
##
"""Provide an inventory of unreleased repository content.
"""

import logging
import os.path

import dateutil.parser
from rcsb.utils.io.FileUtil import FileUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil

logger = logging.getLogger(__name__)


class UnreleasedHoldingsProvider(object):
    """Provide an inventory of unreleased repository content."""

    def __init__(self, cachePath, useCache=False, **kwargs):
        self.__dirPath = os.path.join(cachePath, "holdings")
        self.__storeCache = kwargs.get("storeCache", False)
        self.__filterType = kwargs.get("filterType", "")
        self.__assignDates = "assign-dates" in self.__filterType
        baseUrl = kwargs.get("holdingsTargetUrl", "https://files.wwpdb.org/pub/pdb/holdings")
        fallbackUrl = kwargs.get("holdingsFallbackUrl", "https://files.wwpdb.org/pub/pdb/holdings")
        #
        urlTarget = os.path.join(baseUrl, "unreleased_entries.json.gz")
        urlFallbackTarget = os.path.join(fallbackUrl, "unreleased_entries.json.gz")
        #
        self.__mU = MarshalUtil(workPath=self.__dirPath)
        self.__invD = self.__reload(urlTarget, urlFallbackTarget, self.__dirPath, useCache=useCache)

    def testCache(self, minCount=5000):
        logger.info("Inventory length cD (%d)", len(self.__invD))
        if len(self.__invD) > minCount:
            return True
        return False

    def getStatusCode(self, entryId):
        """Return the status code for the unreleased entry"""
        try:
            return self.__invD[entryId.upper()]["status_code"]
        except Exception as e:
            logger.debug("Failing for %r with %s", entryId, str(e))
        return None

    def getUnreleasedInfo(self, entryId):
        """Return the dictionary describing the details for this unreleased entry"""
        try:
            return self.__invD[entryId.upper()]
        except Exception as e:
            logger.debug("Failing for %r with %s", entryId, str(e))
        return {}

    def getInventory(self):
        """Return the unreleased inventory dictionary"""
        try:
            return self.__invD
        except Exception as e:
            logger.debug("Failing with %s", str(e))
        return {}

    def __reload(self, urlTarget, urlFallbackTarget, dirPath, useCache=True):
        invD = {}
        fU = FileUtil()
        fn = fU.getFileName(urlTarget)
        fp = os.path.join(dirPath, fn)
        self.__mU.mkdir(dirPath)
        #
        if self.__storeCache and useCache and self.__mU.exists(fp):
            invD = self.__mU.doImport(fp, fmt="json")
            logger.debug("Reading cached inventory (%d)", len(invD))
        else:
            invD = self.__mU.doImport(urlTarget, fmt="json")
            logger.info("Loaded inventory from %s (%r)", urlTarget, len(invD))
            if len(invD) == 0:
                invD = self.__mU.doImport(urlFallbackTarget, fmt="json")
                logger.info("Loaded fallback inventory from %s (%r)", urlFallbackTarget, len(invD))
        #
        if self.__storeCache:
            logger.info("Fetch inventory from %s", urlTarget)
            ok = fU.get(urlTarget, fp)
            if not ok:
                ok = fU.get(urlFallbackTarget, fp)
        #
        return invD

    def getStatusDetails(self, curD):
        sD = {}
        for entryId, tD in self.__invD.items():
            if entryId not in curD and tD["status_code"] in ["AUCO", "AUTH", "HOLD", "HPUB", "POLC", "PROC", "REFI", "REPL", "WAIT", "WDRN"]:
                sD[entryId] = {"status": "UNRELEASED", "status_code": tD["status_code"]}
        return sD

    def getRcsbUnreleasedData(self):
        return self.__mapAttributes(self.__invD)

    def __mapAttributes(self, invD):
        #
        mapD = {
            "deposit_date": "deposit_date",
            "title": "title",
            "deposit_date_nmr_constraints": "deposit_date_nmr_restraints",
            "prerelease_sequence_available_flag": "prerelease_sequence_available_flag",
            "deposit_date_structure_factors": "deposit_date_structure_factors",
            "hold_date_structure_factors": "hold_date_structure_factors",
            "author_prerelease_sequence_status": "author_prerelease_sequence_status",
            "deposit_date_coordinates": "deposit_date_coordinates",
            "hold_date_coordinates": "hold_date_coordinates",
            "hold_date_nmr_constraints": "hold_date_nmr_restraints",
            "deposition_authors": "audit_authors",
            "status_code": "status_code",
        }
        dateFields = [
            "deposit_date",
            "deposit_date_coordinates",
            "deposit_date_structure_factors",
            "hold_date_structure_factors",
            "deposit_date_nmr_restraints",
            "hold_date_nmr_restraints",
            "release_date",
            "hold_date_coordinates",
        ]
        retD = {}
        for entryId, tD in invD.items():
            qD = {}
            for iky, oky in mapD.items():
                if iky not in tD:
                    continue
                if self.__assignDates and oky in dateFields:
                    qD[oky] = dateutil.parser.parse(tD[iky])
                elif oky in dateFields:
                    qD[oky] = tD[iky][:10]
                else:
                    qD[oky] = tD[iky]
                #
                if iky == "author_prerelease_sequence_status":
                    qD[oky] = str(qD[oky]).strip().replace("REALEASE", "RELEASE")
            retD[entryId] = qD
        seqD = {}
        for entryId, tD in invD.items():
            if "prerelease_sequence" in tD:
                seqD[entryId] = tD["prerelease_sequence"]
        #
        return retD, seqD
