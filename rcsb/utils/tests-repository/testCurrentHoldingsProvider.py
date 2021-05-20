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
        self.__cachePath = os.path.join(TOPDIR, "CACHE")
        self.__holdingsDirPath = os.path.join(self.__cachePath, "repository")
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
            chP = CurrentHoldingsProvider(holdingsDirPath=self.__holdingsDirPath, useCache=False)
            ok = chP.testCache()
            self.assertTrue(ok)
            cD = chP.getEntryInventory()
            logger.info("current inventory (%d)", len(cD))
            self.assertGreaterEqual(len(cD), listLen)
            #
            ctL = chP.getEntryContentTypes("1kip")
            logger.debug("ctL (%d) %r ", len(ctL), ctL)
            self.assertGreaterEqual(len(ctL), 9)
            #
            for ct in ctL:
                fL = chP.getEntryContentTypePathList("1kip", ct)
                self.assertGreaterEqual(len(fL), 1)
            #
            idList = chP.getEntryIdList()
            self.assertGreaterEqual(len(idList), listLen)

            idList = chP.getEntryIdList(afterDateTimeStamp="2020-01-01")
            logger.info("Ids after 2020 (%d)", len(idList))
            self.assertGreaterEqual(len(idList), 40200)

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
