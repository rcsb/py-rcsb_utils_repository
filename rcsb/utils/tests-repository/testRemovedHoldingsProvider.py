##
# File:    testRemovedHoldingsProvider.py
# Author:  J. Westbrook
# Date:    19-May-2021
# Version: 0.001
#
# Updates:

##
"""
Tests for removed holdings provider.
"""

__docformat__ = "restructuredtext en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"


import logging
import os
import time
import unittest

from rcsb.utils.repository.RemovedHoldingsProvider import RemovedHoldingsProvider

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))


class RemovedHoldingsProviderTests(unittest.TestCase):
    def setUp(self):
        self.__cachePath = os.path.join(HERE, "test-output", "CACHE")
        #
        self.__startTime = time.time()
        logger.debug("Starting %s at %s", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()))

    def tearDown(self):
        endTime = time.time()
        logger.debug("Completed %s at %s (%.4f seconds)\n", self.id(), time.strftime("%Y %m %d %H:%M:%S", time.localtime()), endTime - self.__startTime)

    def testRemoved(self):
        """Test case - get removed holdings"""
        try:
            useCache = False
            rmP = RemovedHoldingsProvider(self.__cachePath, useCache)
            ok = rmP.testCache()
            self.assertTrue(ok)
            cD = rmP.getInventory()
            logger.info("removed inventory (%d)", len(cD))
            self.assertGreaterEqual(len(cD), 5500)

            ctL = rmP.getContentTypes("6irn")
            logger.info("ctL (%d) %r ", len(ctL), ctL)
            self.assertGreaterEqual(len(ctL), 3)
            #
            for ct in ctL:
                fL = rmP.getContentTypePathList("6irn", ct)
                self.assertGreaterEqual(len(fL), 1)
            sc = rmP.getStatusCode("6kpv")
            self.assertEqual(sc, "OBS")
            #
            ctL = rmP.getAllContentTypes()
            logger.info("All removed types: %r", ctL)
            #
            # trsfD, insilicoD, auditAuthorD, removedD, superD = rmP.getRcsbRemovedData()
            _, _, _, removedD, _ = rmP.getRcsbRemovedData()
            #
            kys = list(removedD.keys())
            for ky in kys:
                logger.debug("(%r): %r", ky, removedD[ky])
            self.assertGreaterEqual(len(removedD), 4200)
            #
            scS = set()
            for entryId in rmP.getRemovedEntries():
                sL = rmP.getSupersededBy(entryId)
                if len(sL) > 1:
                    logger.info("(%r) %r", entryId, sL)
                scS.add(rmP.getStatusCode(entryId))
            logger.info("status codes %r", scS)
        except Exception as e:
            logger.exception("Failing with %s", str(e))


def holdingsSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(RemovedHoldingsProviderTests("testRemoved"))
    return suiteSelect


if __name__ == "__main__":
    #
    mySuite = holdingsSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
