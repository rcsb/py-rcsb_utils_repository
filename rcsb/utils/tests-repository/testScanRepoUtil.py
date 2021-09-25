##
# File:    ScanRepoUtilTests.py
# Author:  J. Westbrook
# Date:    1-May-2018
# Version: 0.001
#
# Updates:
#  9-May-2018 jdw add tests for incremental scanning.
# 23-May-2018 jdw simplify dependencies - Get input paths internally in ScanRepoUtil()
# 16-Jun-2018 jdw update DictInfo() prototype
# 18-Jun-2018 jdw update ScanRepoUtil prototype
# 28-Jun-2018 jdw update ScanRepoUtil prototype with workPath
#  2-Jul-2018 jdw remove dependency on mmcif_utils.
# 13-Dec-2018 jdw add IHM support
#  6-Jun-2018 jdw update for changes in DictInfo prototype.
##
"""
Tests for scanning BIRD, CCD and PDBx/mmCIF data files for essential
data type and coverage features.  Generate data sets used to construct schema maps.
"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os
import time
import unittest
from collections import OrderedDict

from mmcif.api.DictionaryApi import DictionaryApi
from rcsb.utils.repository.ScanRepoUtil import ScanRepoUtil
from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class ScanRepoUtilTests(unittest.TestCase):
    def setUp(self):
        #
        #
        mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data")
        configPath = os.path.join(mockTopPath, "config", "dbload-setup-example.yml")
        self.__configName = "site_info_configuration"
        self.__cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=self.__configName, mockTopPath=mockTopPath)
        self.__cachePath = os.path.join(HERE, "test-output", "CACHE")
        self.__pathPdbxDictionary = os.path.join(HERE, "test-data", "mmcif_pdbx_v5_next.dic")

        self.__mU = MarshalUtil(workPath=HERE)

        self.__numProc = 2
        self.__chunkSize = 20
        self.__fileLimit = 20
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testScanRepos(self):
        """Test case - scan ihm data"""
        for contentType in ["chem_comp_core", "bird_chem_comp_core", "bird_family", "pdbx_core", "ihm_dev"]:
            ok = self.__testScanRepo(contentType=contentType, scanType="full")
            self.assertTrue(ok)
            ok = self.__testScanRepo(contentType=contentType, scanType="incr")
            self.assertTrue(ok)

    def __testScanRepo(self, contentType, scanType="full"):
        """Utility method to scan repo for data type and coverage content.

        Using mock repos for tests.
        """
        try:
            containerList = self.__mU.doImport(self.__pathPdbxDictionary, fmt="mmcif-dict")
            dictApi = DictionaryApi(containerList=containerList, consolidate=True, verbose=True)

            failedFilePath = os.path.join(HERE, "test-output", "%s-failed-list-%s.txt" % (contentType, scanType))
            savedFilePath = os.path.join(HERE, "test-output", "%s-path-list-%s.txt" % (contentType, scanType))
            scanDataFilePath = os.path.join(HERE, "test-output", "%s-scan-data.pic" % (contentType))
            dataCoverageFilePath = os.path.join(HERE, "test-output", "%s-scan-data-coverage-%s.json" % (contentType, scanType))
            dataCoverageItemFilePath = os.path.join(HERE, "test-output", "%s-scan-data-item-coverage-%s.tdd" % (contentType, scanType))
            dataTypeFilePath = os.path.join(HERE, "test-output", "%s-scan-data-type-%s.json" % (contentType, scanType))
            #
            ###
            categoryList = sorted(dictApi.getCategoryList())
            dictSchema = {catName: sorted(dictApi.getAttributeNameList(catName)) for catName in categoryList}
            attributeDataTypeD = OrderedDict()
            for catName in categoryList:
                aD = {}
                for atName in dictSchema[catName]:
                    aD[atName] = dictApi.getTypeCode(catName, atName)
                attributeDataTypeD[catName] = aD
            ###
            #
            sr = ScanRepoUtil(
                self.__cfgOb, attributeDataTypeD=attributeDataTypeD, numProc=self.__numProc, chunkSize=self.__chunkSize, fileLimit=self.__fileLimit, workPath=self.__cachePath
            )
            ok = sr.scanContentType(
                contentType, scanType=scanType, inputPathList=None, scanDataFilePath=scanDataFilePath, failedFilePath=failedFilePath, saveInputFileListPath=savedFilePath
            )
            self.assertTrue(ok)
            ok = sr.evalScan(scanDataFilePath, dataTypeFilePath, evalType="data_type")
            self.assertTrue(ok)
            ok = sr.evalScan(scanDataFilePath, dataCoverageFilePath, evalType="data_coverage")
            self.assertTrue(ok)
            ok = sr.evalScanItem(scanDataFilePath, dataCoverageItemFilePath)
            self.assertTrue(ok)

            return ok
        except Exception as e:
            logger.exception("Failing with %s", str(e))
            self.fail()


def scanSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(ScanRepoUtilTests("testScanRepos"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = scanSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
