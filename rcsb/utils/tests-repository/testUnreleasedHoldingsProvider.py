##
# File:    testUnreleasedHoldingsProvider.py
# Author:  J. Westbrook
# Date:    19-May-2021
# Version: 0.001
#
# Updates:

##
"""
Tests for unreleased holdings provider.
"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os
import time
import unittest

from rcsb.utils.repository.UnreleasedHoldingsProvider import UnreleasedHoldingsProvider

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class UnreleasedHoldingsProviderTests(unittest.TestCase):
    def setUp(self):
        self.__cachePath = os.path.join(HERE, "test-output", "CACHE")
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testUnreleased(self):
        """Test case - get unreleased holdings"""
        try:
            rmP = UnreleasedHoldingsProvider(self.__cachePath, useCache=False)
            ok = rmP.testCache()
            self.assertTrue(ok)
            cD = rmP.getInventory()
            logger.info("unreleased inventory (%d)", len(cD))
            self.assertGreaterEqual(len(cD), 18000)
            kS = set()
            for entryId in cD:
                for ky in cD[entryId]:
                    kS.add(ky)
            logger.info("unique keys %r", list(kS))
            #
            sc = rmP.getStatusCode("2a0e")
            self.assertEqual(sc, "WDRN")
            #
            retD, _ = rmP.getRcsbUnreleasedData()
            self.assertGreaterEqual(len(retD), 1000)

        except Exception as e:
            logger.exception("Failing with %s", str(e))

    def testUnreleasedIhm(self):
        """Test case - get unreleased holdings"""
        try:
            rmP = UnreleasedHoldingsProvider(self.__cachePath, useCache=False, repoType="pdb_ihm", storeCache=True)
            ok = rmP.testCache()
            self.assertTrue(ok)
            cD = rmP.getInventory()
            logger.info("unreleased inventory (%d)", len(cD))
            retD, _ = rmP.getRcsbUnreleasedData()
            if len(retD) > 0:
                logger.info("first unreleased data item: %r", next(iter(retD.items())))

        except Exception as e:
            logger.exception("Failing with %s", str(e))


def holdingsSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(UnreleasedHoldingsProviderTests("testUnreleased"))
    suiteSelect.addTest(UnreleasedHoldingsProviderTests("testUnreleasedIhm"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = holdingsSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
