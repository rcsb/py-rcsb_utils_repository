##
# File:    testUpdateHoldingsProvider.py
# Author:  J. Westbrook
# Date:    20-Sep-2021
# Version: 0.001
#
# Updates:

##
"""
Tests for update holdings provider.
"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os
import time
import unittest

from rcsb.utils.repository.UpdateHoldingsProvider import UpdateHoldingsProvider

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class UpdateHoldingsProviderTests(unittest.TestCase):
    def setUp(self):
        self.__cachePath = os.path.join(HERE, "test-output", "CACHE")
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testUpdate(self):
        """Test case - get update holdings"""
        try:
            uhP = UpdateHoldingsProvider(self.__cachePath, useCache=False)
            ok = uhP.testCache()
            self.assertTrue(ok)
            #
        except Exception as e:
            logger.exception("Failing with %s", str(e))


def holdingsSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(UpdateHoldingsProviderTests("testUpdate"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = holdingsSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
