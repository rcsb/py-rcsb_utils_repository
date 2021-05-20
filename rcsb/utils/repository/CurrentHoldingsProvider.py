##
#  File:  CurrentHoldingsProvider.py
#  Date:  18-May-2021 jdw
#
#  Updates:
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

logger = logging.getLogger(__name__)


class CurrentHoldingsProvider(object):
    """Provide inventory of current repository content."""

    def __init__(self, **kwargs):
        self.__dirPath = kwargs.get("holdingsDirPath", ".")
        useCache = kwargs.get("useCache", True)
        baseUrl = kwargs.get("baseUrl", "https://raw.githubusercontent.com/rcsb/py-rcsb_exdb_assets/development/fall_back/holdings/")
        urlTargetContent = kwargs.get("currentTargetUrl", os.path.join(baseUrl, "current_holdings.json.gz"))
        urlFallbackTargetContent = kwargs.get("currentTargetUrl", os.path.join(baseUrl, "current_holdings.json.gz"))
        #
        urlTargetIds = kwargs.get("currentTargetUrl", os.path.join(baseUrl, "current_pdb_ids.json.gz"))
        urlFallbackTargetIds = kwargs.get("currentTargetUrl", os.path.join(baseUrl, "current_pdb_ids.json.gz"))
        #
        self.__mU = MarshalUtil(workPath=self.__dirPath)
        self.__invD = self.__reloadEntryContent(urlTargetContent, urlFallbackTargetContent, self.__dirPath, useCache=useCache)
        self.__idD = self.__reloadEntryIds(urlTargetIds, urlFallbackTargetIds, self.__dirPath, useCache=useCache)

    def testCache(self, minCount=170000):
        logger.info("Inventory length cD (%d) id list (%d)", len(self.__invD), len(self.__idD))
        # TODO - restore consistency checks
        # if len(self.__invD) > minCount and len(self.__idD) > minCount and len(self.__invD) == len(self.__idD):
        if len(self.__invD) > minCount and len(self.__idD) > minCount:
            return True

        return False

    def getEntryContentTypes(self, entryId):
        """Return the current content types for the input entry identifier"""
        try:
            return sorted(self.__invD[entryId.upper()].keys())
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
        return []

    def __reloadEntryContent(self, urlTarget, urlFallbackTarget, dirPath, useCache=True):
        invD = {}
        fU = FileUtil()
        fn = fU.getFileName(urlTarget)
        fp = os.path.join(dirPath, fn)
        self.__mU.mkdir(dirPath)
        #
        if useCache and self.__mU.exists(fp):
            invD = self.__mU.doImport(fp, fmt="json")
            logger.debug("Reading cached inventory (%d)", len(invD))
        else:
            logger.info("Fetch inventory from %s", urlTarget)
            ok = fU.get(urlTarget, fp)
            if not ok:
                ok = fU.get(urlFallbackTarget, fp)
            #
            if ok:
                invD = self.__mU.doImport(fp, fmt="json")
        return invD

    def __reloadEntryIds(self, urlTarget, urlFallbackTarget, dirPath, useCache=True):
        idD = {}
        fU = FileUtil()
        fn = fU.getFileName(urlTarget)
        fp = os.path.join(dirPath, fn)
        self.__mU.mkdir(dirPath)
        #
        if useCache and self.__mU.exists(fp):
            tdL = self.__mU.doImport(fp, fmt="json")
            logger.debug("Reading cached IDs list (%d)", len(tdL))
        else:
            logger.info("Fetch ID list from %s", urlTarget)
            ok = fU.get(urlTarget, fp)
            if not ok:
                ok = fU.get(urlFallbackTarget, fp)
            #
            if ok:
                tdL = self.__mU.doImport(fp, fmt="json")
        #
        for td in tdL:
            for k, v in td.items():
                try:
                    idD[k] = datetime.datetime.fromisoformat(v)
                except Exception as e:
                    logger.error("Date processing failing for %r %r with %s", k, v, str(e))
        #
        sTupL = sorted(idD.items(), key=lambda item: item[1])
        return {k: v for k, v in sTupL}
