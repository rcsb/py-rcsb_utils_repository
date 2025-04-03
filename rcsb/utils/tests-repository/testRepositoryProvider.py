##
# File:    testRepositoryProvider.py
# Author:  J. Westbrook
# Date:    19-Aug-2019
# Version: 0.001
#
# Updates:

##
"""
Tests repository path and object utilities.
"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os
import time
import unittest

from rcsb.utils.repository.RepositoryProvider import RepositoryProvider
from rcsb.utils.config.ConfigUtil import ConfigUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class RepositoryProviderTests(unittest.TestCase):
    def setUp(self):
        #
        self.__mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(self.__mockTopPath, "config", "dbload-setup-example.yml")
        self.__configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=self.__configName, mockTopPath=self.__mockTopPath)
        self.__cachePath = os.path.join(HERE, "test-output", "CACHE")

        self.__numProc = 2
        self.__fileLimit = 20
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testLocalRepoUtils(self):
        """Test case - repository locator local path utilities"""
        rpP = RepositoryProvider(cfgOb=self.__cfgOb, discoveryMode="local", numProc=self.__numProc, fileLimit=self.__fileLimit, cachePath=self.__cachePath)
        for contentType in ["bird_chem_comp_core", "pdbx_core", "ihm_dev"]:
            logger.info("test contentType %s", contentType)
            mergeContentTypes = None
            if contentType in ["pdbx_core"]:
                mergeContentTypes = ["vrpt"]
            #
            locatorObjList = rpP.getLocatorObjList(contentType=contentType, mergeContentTypes=mergeContentTypes)
            pathList = rpP.getLocatorPaths(locatorObjList)
            locatorObjList2 = rpP.getLocatorsFromPaths(locatorObjList, pathList)
            logger.info("%s pathList length %d", contentType, len(pathList))
            self.assertEqual(len(locatorObjList), len(pathList))
            self.assertEqual(len(locatorObjList), len(locatorObjList2))
            if contentType == "pdbx_core":
                logger.info("pdbx-core locators %d", len(locatorObjList))
                containerList = rpP.getContainerList(locatorObjList)
                logger.info("pdbx-core containerList (%d)", len(containerList))
                self.assertEqual(len(containerList), len(locatorObjList))
                for container in containerList:
                    logger.info("container %r category names - (%d)", container.getName(), len(container.getObjNameList()))
                    logger.debug("category names - (%r)", container.getObjNameList())
                    self.assertGreaterEqual(len(container.getObjNameList()), 50)

        for contentType in ["bird_chem_comp_core", "pdbx_core", "ihm_dev"]:
            logger.info("test contentType %s", contentType)
            mergeContentTypes = None
            if contentType in ["pdbx_core"]:
                mergeContentTypes = ["vrpt"]
            #
            locatorObjList = rpP.getLocatorObjList(contentType=contentType, mergeContentTypes=mergeContentTypes)
            logger.info("locatorObjList len %r", len(locatorObjList))
            logger.debug("locatorObjList %r", locatorObjList)
            pathList = rpP.getLocatorPaths(locatorObjList)
            self.assertEqual(len(locatorObjList), len(pathList))
            #
            idCodes = rpP.getLocatorIdcodes(contentType, locatorObjList)
            logger.debug("idCodes %r", idCodes)
            self.assertEqual(len(locatorObjList), len(idCodes))
            excludeList = idCodes[: int(len(idCodes) / 2)]
            logger.info("excludeList (%d) first few: %r", len(excludeList), excludeList[0:3])
            logger.debug("excludeList %r", excludeList)
            fL = rpP.getLocatorObjList(contentType=contentType, mergeContentTypes=mergeContentTypes, excludeIds=excludeList)
            logger.info("fL (%d) first few: %r", len(fL), fL[0:3])
            logger.debug("fL %r", fL)
            fLidCodes = rpP.getLocatorIdcodes(contentType, fL)
            logger.info("fLidCodes %r", fLidCodes)
            # Compare the returned set of ID codes with the excluded ID codes - should be empty
            # Note that must use this for comparison since using "applyLimit" can result in different sets of
            # IDs being returned depending on the order that the program assembles them into a list
            excludedInfL = [id for id in excludeList if id in fLidCodes]
            logger.info("excludedInfL %r", excludedInfL)
            self.assertEqual(len(excludedInfL), 0)

    def testRemoteRepoUtils(self):
        """Test case - repository remote locator uri utilities"""
        rpP = RepositoryProvider(cfgOb=self.__cfgOb, numProc=self.__numProc, fileLimit=self.__fileLimit, cachePath=self.__cachePath, discoveryMode="remote")
        for contentType in ["bird_chem_comp_core", "pdbx_core"]:
            logger.info("test contentType %s", contentType)
            mergeContentTypes = None
            if contentType in ["pdbx_core"]:
                mergeContentTypes = ["vrpt"]
            #
            locatorObjList = rpP.getLocatorObjList(contentType=contentType, mergeContentTypes=mergeContentTypes)
            pathList = rpP.getLocatorPaths(locatorObjList)
            locatorObjList2 = rpP.getLocatorsFromPaths(locatorObjList, pathList)
            logger.info("contentType %s pathList length %d", contentType, len(pathList))
            logger.info("locatorObjList %r", locatorObjList)
            self.assertEqual(len(locatorObjList), len(pathList))
            self.assertEqual(len(locatorObjList), len(locatorObjList2))
            #
            # Test excludeIds
            lCount = len(pathList)
            idCodes = rpP.getLocatorIdcodes(contentType, locatorObjList)
            self.assertEqual(len(locatorObjList), len(idCodes))
            excludeList = idCodes[: int(len(idCodes) / 2)]
            logger.info("excludeList (%d) first few: %r", len(excludeList), excludeList[0:3])
            fL = rpP.getLocatorObjList(contentType=contentType, mergeContentTypes=mergeContentTypes, excludeIds=excludeList)
            logger.info("fL (%d) first few: %r", len(fL), fL[0:3])
            self.assertEqual(lCount, len(fL) + len(excludeList))

            if contentType == "pdbx_core":
                logger.info("pdbx-core locators (%d) first one: %r", len(locatorObjList), locatorObjList[0])
                logger.debug("pdbx-core locators: %r", locatorObjList)
                containerList = rpP.getContainerList(locatorObjList)
                logger.info("pdbx-core containerList (%d) first one: %r", len(containerList), containerList[0].getName())
                logger.debug("pdbx-core containerList: %r", containerList)
                self.assertEqual(len(containerList), len(locatorObjList))
                for container in containerList:
                    logger.info("container %r category names - (%d)", container.getName(), len(container.getObjNameList()))
                    logger.debug("category names - (%r)", container.getObjNameList())
                    self.assertGreaterEqual(len(container.getObjNameList()), 50)

    def testRemoteSelectedRepoUtilsIds(self):
        rpP = RepositoryProvider(cfgOb=self.__cfgOb, numProc=self.__numProc, fileLimit=None, cachePath=self.__cachePath, discoveryMode="remote")
        locatorObjList = rpP.getLocatorObjList(inputIdCodeList=["1kip", "4hhb"], contentType="pdbx_core", mergeContentTypes=["vrpt"])
        logger.info("locatorObjList %r", locatorObjList)
        self.assertEqual(len(locatorObjList), 2)
        pathList = rpP.getLocatorPaths(locatorObjList)
        logger.info("pathList %r", pathList)
        for loc in locatorObjList:
            self.assertEqual(len(loc), 2)
        containerList = rpP.getContainerList(locatorObjList)
        logger.info("pdbx-core containerList (%d) first one: %r", len(containerList), containerList[0].getName())
        logger.debug("pdbx-core containerList: %r", containerList)
        #
        locatorObjList = rpP.getLocatorObjList(inputIdCodeList=["ATP", "GTP", "A1A3S"], contentType="chem_comp")
        logger.info("locatorObjList %r", locatorObjList)
        pathList = rpP.getLocatorPaths(locatorObjList)
        logger.info("pathList %r", pathList)
        self.assertEqual(len(locatorObjList), 3)
        containerList = rpP.getContainerList(locatorObjList)
        logger.info("chem_comp containerList (%d) first one: %r", len(containerList), containerList[0].getName())
        logger.debug("chem_comp containerList: %r", containerList)

    def testRemoteSelectedRepoUtilsPaths(self):
        inputPathList = [
            os.path.join(self.__mockTopPath, "MOCK_PDBX_SANDBOX", "ds/1dsr/1dsr.cif.gz"),
            "https://files.wwpdb.org/pub/pdb/data/structures/divided/mmCIF/hh/4hhb.cif.gz"
        ]
        rpP = RepositoryProvider(cfgOb=self.__cfgOb, numProc=self.__numProc, fileLimit=None, cachePath=self.__cachePath, discoveryMode="remote")
        locatorObjList = rpP.getLocatorObjList(inputPathList=inputPathList, contentType="pdbx_core", mergeContentTypes=["vrpt"])
        logger.info("locatorObjList %r", locatorObjList)
        self.assertEqual(len(locatorObjList), 2)
        pathList = rpP.getLocatorPaths(locatorObjList)
        logger.info("pathList %r", pathList)
        for loc in locatorObjList:
            self.assertEqual(len(loc), 2)
        containerList = rpP.getContainerList(locatorObjList)
        logger.info("pdbx-core containerList (%d) first one: %r", len(containerList), containerList[0].getName())
        logger.debug("pdbx-core containerList: %r", containerList)
        #
        #
        inputPathList = [
            os.path.join(self.__mockTopPath, "MOCK_CHEM_COMP_REPO", "A/ATP/ATP.cif"),
            "https://files.wwpdb.org/pub/pdb/refdata/chem_comp/P/GTP/GTP.cif",
            "https://files.wwpdb.org/pub/pdb/refdata/chem_comp/S/A1A3S/A1A3S.cif"
        ]
        locatorObjList = rpP.getLocatorObjList(inputPathList=inputPathList, contentType="chem_comp")
        logger.info("locatorObjList %r", locatorObjList)
        pathList = rpP.getLocatorPaths(locatorObjList)
        logger.info("pathList %r", pathList)
        self.assertEqual(len(locatorObjList), 3)
        containerList = rpP.getContainerList(locatorObjList)
        logger.info("chem_comp containerList (%d) first one: %r", len(containerList), containerList[0].getName())
        logger.debug("chem_comp containerList: %r", containerList)


def repoSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(RepositoryProviderTests("testLocalRepoUtils"))
    suiteSelect.addTest(RepositoryProviderTests("testRemoteRepoUtils"))
    suiteSelect.addTest(RepositoryProviderTests("testRemoteSelectedRepoUtilsIds"))
    suiteSelect.addTest(RepositoryProviderTests("testRemoteSelectedRepoUtilsPaths"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = repoSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
