##
# File:    testCurrentHoldingsProvider.py
# Author:  J. Westbrook
# Date:    17-May-2021
# Version: 0.001
#
# Updates:

##
"""
Tests for current holdings provider.
"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os
import time
import unittest

from rcsb.utils.io.FileUtil import FileUtil
from rcsb.utils.repository.CurrentHoldingsProvider import CurrentHoldingsProvider

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class CurrentHoldingsProviderTests(unittest.TestCase):
    def setUp(self):
        #
        #
        self.__cachePath = os.path.join(HERE, "test-output", "CACHE")
        self.__dataPath = os.path.join(HERE, "test-data")
        #
        # Mock the EntryInfo cache -
        fU = FileUtil()
        fU.put(os.path.join(self.__dataPath, "entry_info_details.json"), os.path.join(self.__cachePath, "rcsb_entry_info", "entry_info_details.json"))
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testEntryCurrent(self):
        """Test case - get current holdings"""
        try:
            listLen = 177400
            useCache = False
            chP = CurrentHoldingsProvider(self.__cachePath, useCache)
            ok = chP.testCache()
            ctL = chP.getAllContentTypes()
            logger.info("contentTypes %r", ctL)
            self.assertTrue(ok)
            cD = chP.getEntryInventory()
            logger.info("current inventory (%d)", len(cD))
            self.assertGreaterEqual(len(cD), listLen)
            #
            entryId = "1kip"
            ctL = chP.getEntryContentTypes(entryId)
            logger.info("ctL (%d) %r ", len(ctL), ctL)
            self.assertGreaterEqual(len(ctL), 8)
            #
            for ct in ctL:
                fL = chP.getEntryContentTypePathList(entryId, ct)
                if "map" not in ct.lower():
                    self.assertGreaterEqual(len(fL), 1)
            #
            idList = chP.getEntryIdList()
            self.assertGreaterEqual(len(idList), listLen)

            idList = chP.getEntryIdList(afterDateTimeStamp="2020-01-01")
            logger.info("Ids after 2020 (%d)", len(idList))
            self.assertGreaterEqual(len(idList), 40200)
            #
            ctD, assemD = chP.getRcsbContentAndAssemblies()
            logger.info("ctD (%d) assemD (%d)", len(ctD), len(assemD))
            logger.info("ctD for entryId %s: %r", entryId, ctD[entryId.upper()])
            logger.info("assemD for entryId %s: %r", entryId, assemD[entryId.upper()])

        except Exception as e:
            logger.exception("Failing with %s", str(e))


def holdingsSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(CurrentHoldingsProviderTests("testEntryCurrent"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = holdingsSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
