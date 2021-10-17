##
# File:    testRepositoryModelProvider.py
# Author:  J. Westbrook
# Date:    1-Oct-2021
# Version: 0.001
#
# Updates:

##
"""
Tests repository path and object utilities for structure models.
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
from rcsb.utils.insilico3d.AlphaFoldModelProvider import AlphaFoldModelProvider
from rcsb.utils.io.FileUtil import FileUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class RepositoryModelProviderTests(unittest.TestCase):
    doModelTests = False

    def setUp(self):
        #
        #
        mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(mockTopPath, "config", "dbload-setup-example.yml")
        self.__configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=self.__configName, mockTopPath=mockTopPath)
        self.__cachePath = os.path.join(HERE, "test-output", "CACHE")
        self.__fileLimit = None
        self.__rpP = RepositoryProvider(cfgOb=self.__cfgOb, fileLimit=self.__fileLimit, cachePath=self.__cachePath)
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    @unittest.skipUnless(doModelTests, "Skip model tests for now")
    def testGetModelPaths(self):
        """Test case - get model locator path utilities"""
        aFMP = AlphaFoldModelProvider(cachePath=self.__cachePath, useCache=True, alphaFoldRequestedSpeciesList=["Staphylococcus aureus"])
        ok = aFMP.testCache()
        self.assertTrue(ok)
        ok = aFMP.reorganizeModelFiles()
        fU = FileUtil()
        if ok and fU.exists(os.path.join(self.__cachePath, "divided")):
            fU.replace(os.path.join(self.__cachePath, "divided"), os.path.join(self.__cachePath, "computed-models"))
            fU.remove(os.path.join(self.__cachePath, "AlphaFold"))

        for contentType in ["pdbx_core_model_core"]:
            mergeContentTypes = None
            #
            locatorObjList = self.__rpP.getLocatorObjList(contentType=contentType, mergeContentTypes=mergeContentTypes)
            pathList = self.__rpP.getLocatorPaths(locatorObjList)
            logger.info("pathList (%d)", len(pathList))


def repoSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(RepositoryModelProviderTests("testGetModelPaths"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = repoSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
