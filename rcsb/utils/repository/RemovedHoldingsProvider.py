##
#  File:  RemovedHoldingsProvider.py
#  Date:  18-May-2021 jdw
#
#  Updates:
#
##
"""Provide an inventory of removed repository content.
"""

import logging
import os.path

from rcsb.utils.io.FileUtil import FileUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil

logger = logging.getLogger(__name__)


class RemovedHoldingsProvider(object):
    """Provide an inventory of removed repository content."""

    def __init__(self, **kwargs):
        self.__dirPath = kwargs.get("holdingsDirPath", ".")
        useCache = kwargs.get("useCache", True)
        baseUrl = kwargs.get("baseUrl", "https://raw.githubusercontent.com/rcsb/py-rcsb_exdb_assets/development/fall_back/holdings/")
        urlTarget = kwargs.get("removedTargetUrl", os.path.join(baseUrl, "removed_holdings.json.gz"))
        urlFallbackTarget = kwargs.get("removedTargetUrl", os.path.join(baseUrl, "removed_holdings.json.gz"))
        #
        self.__mU = MarshalUtil(workPath=self.__dirPath)
        self.__invD = self.__reload(urlTarget, urlFallbackTarget, self.__dirPath, useCache=useCache)

    def testCache(self, minCount=1000):
        logger.info("Inventory length cD (%d)", len(self.__invD))
        if len(self.__invD) > minCount:
            return True
        return False

    def getStatusCode(self, entryId):
        """Return the status code for the removed entry"""
        try:
            return self.__invD[entryId.upper()]["status_code"]
        except Exception as e:
            logger.debug("Failing for %r with %s", entryId, str(e))
        return None

    def getRemovedInfo(self, entryId):
        """Return the dictionary describing the details for this removed entry"""
        try:
            return self.__invD[entryId.upper()]
        except Exception as e:
            logger.debug("Failing for %r with %s", entryId, str(e))
        return {}

    def getContentTypes(self, entryId):
        """Return the removed content types for the input entry identifier"""
        try:
            return sorted(self.__invD[entryId.upper()]["content_type"].keys())
        except Exception as e:
            logger.debug("Failing for %r with %s", entryId, str(e))
        return []

    def getContentTypePathList(self, entryId, contentType):
        """Return the removed content types for the input entry identifier"""
        try:
            return (
                self.__invD[entryId.upper()]["content_type"][contentType]
                if isinstance(self.__invD[entryId.upper()]["content_type"][contentType], list)
                else [self.__invD[entryId.upper()]["content_type"][contentType]]
            )
        except Exception as e:
            logger.debug("Failing for %r %r with %s", entryId, contentType, str(e))
        return []

    def getInventory(self):
        """Return the removed inventory dictionary"""
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
        #
        return invD
