##
#  File:  RepositoryHelper.py
#  Date:  18-Nov-2026 dwp
#
#  Updates:
#
##
"""Helpers for addressing PDB archive content through short links, and for entry identifier case.

The short links (served by the RCSB jshortlink service) are the same on the current (classic) archive and on the
extended-id (beta) archive, so code built on them does not need to know the archive layout:

    <root>/download/<id>.cif.gz                                  entry (released, obsolete or IHM)
    <root>/validation/download/<id>_validation.cif.gz            validation report
    <root>/ligands/download/<CCD id>.cif                         chemical component
    <root>/birds/download/<PRD_, PRDCC_ or FAM_ id>.cif          BIRD
    <root>/holdings/download/<group>/<file name>                 holdings, group: "pdb", "pdb_ihm" or "refdata"

where <root> is the server root, e.g. "https://files.wwpdb.org" (no "/pub").
"""

import logging
from urllib.parse import urlsplit

logger = logging.getLogger(__name__)

DEFAULT_ARCHIVE_ROOT_URL = "https://files.wwpdb.org"

HOLDINGS_SHORTLINK_PATH = "holdings/download"


def getRepositoryRootUrl(repoUrl, default=DEFAULT_ARCHIVE_ROOT_URL):
    """Return the archive server root (scheme and host, e.g. "https://files.wwpdb.org") for a configured PDB_REPO_URL.

    Short links live at the server root, so any path in the URL is dropped: PDB_REPO_URL may name the server root
    ("https://files.wwpdb.org") or, as in the configs for the current archive, its "pub" directory
    ("https://files.wwpdb.org/pub").

    Args:
        repoUrl (str): configured repository URL (may be None or empty)
        default (str): value used when repoUrl is not set

    Returns:
        str: server root URL (no path, no trailing slash), or the input if it has no scheme and host
    """
    url = (repoUrl or default or "").strip()
    parts = urlsplit(url)
    if not parts.scheme or not parts.netloc:
        logger.warning("Repository URL %r has no scheme and host, using it as given", url)
        return url
    return parts.scheme + "://" + parts.netloc


def getHoldingsUrl(rootUrl, group="pdb"):
    """Return the holdings short link directory for a group ("pdb", "pdb_ihm" or "refdata")."""
    return "/".join([getRepositoryRootUrl(rootUrl), HOLDINGS_SHORTLINK_PATH, group])


def getRepoTypeHoldingsUrl(holdingsUrl, repoType="pdb"):
    """Return the holdings directory URL for a repository type, given the PDB holdings short link directory.

    The "pdb" holdings short link (".../holdings/download/pdb") becomes the "pdb_ihm" one for repoType "pdb_ihm".
    Any other URL is returned as given, so a caller that passes its own holdings directory must pass the one
    for the repository type it asks for.

    Args:
        holdingsUrl (str): holdings directory URL
        repoType (str): "pdb" or "pdb_ihm"

    Returns:
        str: holdings directory URL for repoType
    """
    if repoType != "pdb_ihm" or not holdingsUrl:
        return holdingsUrl
    url = holdingsUrl.rstrip("/")
    shortlinkPdb = "/" + HOLDINGS_SHORTLINK_PATH + "/pdb"
    if url.endswith(shortlinkPdb):
        return url[: -len("pdb")] + "pdb_ihm"
    return holdingsUrl


def normalizeEntryId(entryId):
    """Return an entry identifier in the case the archive holdings use for its format.

    Extended PDB ids ("pdb_00004hhb") are lower case; short PDB ids ("4HHB") are upper case. Holdings of the
    current archive are keyed by upper case short ids, holdings of the extended-id archive by lower case
    extended ids, so lookups normalize the input this way instead of relying on the caller's case.
    """
    if not entryId:
        return entryId
    entryId = entryId.strip()
    return entryId.lower() if entryId.lower().startswith("pdb_") else entryId.upper()
