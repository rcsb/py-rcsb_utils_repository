##
# File:    testRepositoryHelper.py
# Author:  D. Piehl
# Date:    18-Nov-2026
#
# Updates:
#
##
"""
Tests for the archive short link URL and entry id helpers (no network access).
"""

import logging
import unittest

from rcsb.utils.repository.RepositoryHelper import getRepositoryRootUrl, getHoldingsUrl, getRepoTypeHoldingsUrl, normalizeEntryId

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()


class RepositoryHelperTests(unittest.TestCase):
    def testRepositoryRootUrl(self):
        # current archive configs name the "pub" dir, extended-id archive configs the server root
        self.assertEqual(getRepositoryRootUrl("https://files.wwpdb.org/pub"), "https://files.wwpdb.org")
        self.assertEqual(getRepositoryRootUrl("https://files.wwpdb.org/pub/"), "https://files.wwpdb.org")
        self.assertEqual(getRepositoryRootUrl("http://shortlink-internal-a.production.svc.cluster.local/pub"), "http://shortlink-internal-a.production.svc.cluster.local")
        self.assertEqual(getRepositoryRootUrl("http://shortlink-beta-a.production.svc.cluster.local"), "http://shortlink-beta-a.production.svc.cluster.local")
        self.assertEqual(getRepositoryRootUrl("https://files-beta.wwpdb.org/"), "https://files-beta.wwpdb.org")
        # unset falls back to the default
        self.assertEqual(getRepositoryRootUrl(None), "https://files.wwpdb.org")
        self.assertEqual(getRepositoryRootUrl("", default="https://files-beta.wwpdb.org"), "https://files-beta.wwpdb.org")
        # only scheme and host are kept (incl. a port)
        self.assertEqual(getRepositoryRootUrl("http://localhost:18080/pub"), "http://localhost:18080")
        self.assertEqual(getRepositoryRootUrl("https://example.org/some/prefix/"), "https://example.org")

    def testHoldingsUrls(self):
        pdbUrl = getHoldingsUrl("https://files.wwpdb.org/pub", "pdb")
        self.assertEqual(pdbUrl, "https://files.wwpdb.org/holdings/download/pdb")
        self.assertEqual(getHoldingsUrl("https://files-beta.wwpdb.org", "pdb_ihm"), "https://files-beta.wwpdb.org/holdings/download/pdb_ihm")
        # repository type switch for the short link ...
        self.assertEqual(getRepoTypeHoldingsUrl(pdbUrl, "pdb_ihm"), "https://files.wwpdb.org/holdings/download/pdb_ihm")
        self.assertEqual(getRepoTypeHoldingsUrl(pdbUrl, "pdb"), pdbUrl)
        # ... any other directory URL is the caller's own, used as given
        self.assertEqual(getRepoTypeHoldingsUrl("https://files.wwpdb.org/pub/pdb_ihm/holdings", "pdb_ihm"), "https://files.wwpdb.org/pub/pdb_ihm/holdings")
        betaRoot = "http://shortlink-beta-a.production.svc.cluster.local"
        self.assertEqual(getHoldingsUrl(betaRoot, "refdata"), betaRoot + "/holdings/download/refdata")

    def testNormalizeEntryId(self):
        self.assertEqual(normalizeEntryId("4hhb"), "4HHB")
        self.assertEqual(normalizeEntryId(" 4HHB "), "4HHB")
        self.assertEqual(normalizeEntryId("pdb_00004hhb"), "pdb_00004hhb")
        self.assertEqual(normalizeEntryId("PDB_00004HHB"), "pdb_00004hhb")
        self.assertIsNone(normalizeEntryId(None))
        self.assertEqual(normalizeEntryId(""), "")


def repositoryHelperSuite():
    suiteSelect = unittest.TestSuite()
    suiteSelect.addTest(RepositoryHelperTests("testRepositoryRootUrl"))
    suiteSelect.addTest(RepositoryHelperTests("testHoldingsUrls"))
    suiteSelect.addTest(RepositoryHelperTests("testNormalizeEntryId"))
    return suiteSelect


if __name__ == "__main__":
    mySuite = repositoryHelperSuite()
    unittest.TextTestRunner(verbosity=2).run(mySuite)
