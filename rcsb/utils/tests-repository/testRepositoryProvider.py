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
        #
        mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(mockTopPath, "config", "dbload-setup-example.yml")
        self.__configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=self.__configName, mockTopPath=mockTopPath)
        self.__cachePath = os.path.join(HERE, "test-output", "CACHE")

        self.__numProc = 2
        self.__chunkSize = 20
        self.__fileLimit = None
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
                    logger.debug("category names - (%d)", len(container.getObjNameList()))
                    self.assertGreaterEqual(len(container.getObjNameList()), 50)
            #
        for contentType in ["bird_chem_comp_core", "pdbx_core", "ihm_dev"]:
            mergeContentTypes = None
            if contentType in ["pdbx_core"]:
                mergeContentTypes = ["vrpt"]
            #
            locatorObjList = rpP.getLocatorObjList(contentType=contentType, mergeContentTypes=mergeContentTypes)
            pathList = rpP.getLocatorPaths(locatorObjList)
            self.assertEqual(len(locatorObjList), len(pathList))
            #
            lCount = len(pathList)
            idCodes = rpP.getLocatorIdcodes(contentType, locatorObjList)
            self.assertEqual(len(locatorObjList), len(idCodes))
            excludeList = idCodes[: int(len(idCodes) / 2)]
            logger.debug("excludeList (%d) %r", len(excludeList), excludeList)
            fL = rpP.getLocatorObjList(contentType=contentType, mergeContentTypes=mergeContentTypes, excludeIds=excludeList)
            logger.debug("fL (%d)", len(fL))
            self.assertEqual(lCount, len(fL) + len(excludeList))

    def testRemoteRepoUtils(self):
        """Test case - repository remote locator uri utilities"""
        fileLimit = 20
        rpP = RepositoryProvider(cfgOb=self.__cfgOb, discoveryMode="remote", numProc=self.__numProc, fileLimit=fileLimit, cachePath=self.__cachePath)
        for contentType in ["bird_chem_comp_core", "pdbx_core"]:
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
                # logger.debug("pdbx-core locators %r", locatorObjList)
                containerList = rpP.getContainerList(locatorObjList)
                logger.info("pdbx-core containerList (%d)", len(containerList))
                # logger.debug("pdbx-core containerList (%r)", containerList)
                self.assertEqual(len(containerList), len(locatorObjList))
                for container in containerList:
                    logger.debug("category names - (%d)", len(container.getObjNameList()))
                    logger.debug("category names - (%r)", container.getObjNameList())
                    self.assertGreaterEqual(len(container.getObjNameList()), 50)
            #
        for contentType in ["bird_chem_comp_core", "pdbx_core"]:
            mergeContentTypes = None
            if contentType in ["pdbx_core"]:
                mergeContentTypes = ["vrpt"]
            #
            locatorObjList = rpP.getLocatorObjList(contentType=contentType, mergeContentTypes=mergeContentTypes)
            pathList = rpP.getLocatorPaths(locatorObjList)
            self.assertEqual(len(locatorObjList), len(pathList))
            logger.info("locatorObjList %r", locatorObjList)
            #
            lCount = len(pathList)
            idCodes = rpP.getLocatorIdcodes(contentType, locatorObjList)
            self.assertEqual(len(locatorObjList), len(idCodes))
            excludeList = idCodes[: int(len(idCodes) / 2)]
            logger.debug("excludeList (%d) %r", len(excludeList), excludeList)
            fL = rpP.getLocatorObjList(contentType=contentType, mergeContentTypes=mergeContentTypes, excludeIds=excludeList)
            logger.debug("fL (%d)", len(fL))
            self.assertEqual(lCount, len(fL) + len(excludeList))

    def testRemoteSelectedRepoUtils(self):
        rpP = RepositoryProvider(cfgOb=self.__cfgOb, discoveryMode="remote", numProc=self.__numProc, fileLimit=None, cachePath=self.__cachePath)
        locL = rpP.getLocatorObjList(inputIdCodeList=["1kip", "4hhb"], contentType="pdbx_core", mergeContentTypes=["vrpt"])
        self.assertEqual(len(locL), 2)
        for loc in locL:
            self.assertEqual(len(loc), 2)
        #
        locL = rpP.getLocatorObjList(inputIdCodeList=["ATP", "GTP"], contentType="chem_comp")
        self.assertEqual(len(locL), 2)


def repoSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(RepositoryProviderTests("testLocalRepoUtils"))
    suiteSelect.addTest(RepositoryProviderTests("testRemoteRepoUtils"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = repoSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
