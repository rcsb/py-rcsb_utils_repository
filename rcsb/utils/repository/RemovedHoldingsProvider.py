##
#  File:  RemovedHoldingsProvider.py
#  Date:  18-May-2021 jdw
#
#  Updates:
#   18-Apr-2022  dwp Update getSupersededBy method to recursively return all superseded entries
##
"""Provide an inventory of removed repository content.
"""
import logging
import os.path

import dateutil.parser
from rcsb.utils.io.FileUtil import FileUtil
from rcsb.utils.io.MarshalUtil import MarshalUtil

logger = logging.getLogger(__name__)


class RemovedHoldingsProvider(object):
    """Provide an inventory of removed repository content."""

    def __init__(self, cachePath, useCache, **kwargs):
        self.__cachePath = cachePath
        self.__dirPath = os.path.join(self.__cachePath, "holdings")
        self.__filterType = kwargs.get("filterType", "")
        self.__assignDates = "assign-dates" in self.__filterType
        baseUrl = kwargs.get("holdingsTargetUrl", "https://ftp.wwpdb.org/pub/pdb/holdings")
        fallbackUrl = kwargs.get("holdingsFallbackUrl", "https://ftp.wwpdb.org/pub/pdb/holdings")
        #
        urlTarget = os.path.join(baseUrl, "all_removed_entries.json.gz")
        urlFallbackTarget = os.path.join(fallbackUrl, "all_removed_entries.json.gz")
        #
        self.__mU = MarshalUtil(workPath=self.__dirPath)
        self.__invD = self.__reload(urlTarget, urlFallbackTarget, self.__dirPath, useCache=useCache)

    def testCache(self, minCount=1000):
        logger.info("Inventory length cD (%d)", len(self.__invD))
        if len(self.__invD) > minCount:
            return True
        return False

    def getStatusCode(self, entryId):
        """Return the status code for the removed entry"""
        try:
            return self.__invD[entryId.upper()]["status_code"]
        except Exception as e:
            logger.debug("Failing for %r with %s", entryId, str(e))
        return None

    def getSupersededBy(self, entryId):
        """Return the superseding entry ids"""
        try:
            sL = []
            if isinstance(self.__invD[entryId.upper()]["superseded_by"], str):
                sL = [self.__invD[entryId.upper()]["superseded_by"]]
            else:
                sL = self.__invD[entryId.upper()]["superseded_by"]
            if len(sL) > 0:
                for recursiveEntry in sL:
                    if isinstance(self.__invD[recursiveEntry.upper()]["superseded_by"], str):
                        sL = sL + [self.__invD[recursiveEntry.upper()]["superseded_by"]]
                    elif isinstance(self.__invD[recursiveEntry.upper()]["superseded_by"], list):
                        sL = sL + self.__invD[recursiveEntry.upper()]["superseded_by"]
                    else:
                        break
        except Exception as e:
            logger.debug("Failing for %r with %s", entryId, str(e))
        return sL

    def getRemovedEntries(self):
        return list(self.__invD.keys())

    def getEntryByStatus(self, statusCode):
        """Return the entry codes for removed entries with the input status code"""
        try:
            return [entryId for entryId, vD in self.__invD.items() if "status_code" in vD and vD["status_code"] == statusCode]
        except Exception as e:
            logger.debug("Failing for %r with %s", statusCode, str(e))
        return []

    def getRemovedInfo(self, entryId):
        """Return the dictionary describing the details for this removed entry"""
        try:
            return self.__invD[entryId.upper()]
        except Exception as e:
            logger.debug("Failing for %r with %s", entryId, str(e))
        return {}

    def getContentTypes(self, entryId):
        """Return the removed content types for the input entry identifier"""
        try:
            return sorted(self.__invD[entryId.upper()]["content_type"].keys())
        except Exception as e:
            logger.debug("Failing for %r with %s", entryId, str(e))
        return []

    def getContentTypePathList(self, entryId, contentType):
        """Return the removed content types for the input entry identifier"""
        try:
            return (
                self.__invD[entryId.upper()]["content_type"][contentType]
                if isinstance(self.__invD[entryId.upper()]["content_type"][contentType], list)
                else [self.__invD[entryId.upper()]["content_type"][contentType]]
            )
        except Exception as e:
            logger.debug("Failing for %r %r with %s", entryId, contentType, str(e))
        return []

    def getInventory(self):
        """Return the removed inventory dictionary"""
        try:
            return self.__invD
        except Exception as e:
            logger.debug("Failing with %s", str(e))
        return {}

    def getAllContentTypes(self):
        """Return the removed content types for the input entry identifier"""
        entryId = None
        try:
            cS = set()
            for entryId, tD in self.__invD.items():
                if "content_type" in tD:
                    for cT in tD["content_type"].keys():
                        cS.add(cT)
            return sorted(list(cS))
        except Exception as e:
            logger.exception("Failing for %r with %s", entryId, str(e))
        return []

    def __reload(self, urlTarget, urlFallbackTarget, dirPath, useCache=True):
        invD = {}
        fU = FileUtil()
        fn = fU.getFileName(urlTarget)
        fp = os.path.join(dirPath, fn)
        self.__mU.mkdir(dirPath)
        #
        if useCache and self.__mU.exists(fp):
            invD = self.__mU.doImport(fp, fmt="json")
            logger.info("Reading cached inventory (%d) from file %s", len(invD), fp)
        else:
            logger.info("Fetch inventory from %s", urlTarget)
            ok = fU.get(urlTarget, fp)
            if not ok:
                ok = fU.get(urlFallbackTarget, fp)
            #
            if ok:
                invD = self.__mU.doImport(fp, fmt="json")
        #
        return invD

    def getStatusDetails(self, curD):
        rmD = {}
        for entryId, tD in self.__invD.items():
            if entryId in curD:
                continue
            #
            if tD["status_code"] == "TRSF":
                rmD[entryId] = {"status": "REMOVED", "status_code": "TRSF"}
            elif tD["status_code"] == "OBS":
                sL = self.getSupersededBy(entryId)
                if sL:
                    if sL[-1] in curD:
                        rmD[entryId] = {"status": "REMOVED", "status_code": "OBS", "id_code_replaced_by_latest": sL[-1]}
                    else:
                        rmD[entryId] = {"status": "REMOVED", "status_code": "OBS"}
                else:
                    rmD[entryId] = {"status": "REMOVED", "status_code": "OBS"}
        return rmD

    def getRcsbRemovedData(self):
        """Assemble and reorganize removed data for RCSB loading operations.

        Returns:
            dict: dictionary of transferred data,
            dict: dictionary of removed onsilico data,
            dict: dictioary of audit authors of removed data,
            dict: dictionary of all removed data,
            dict: dictionary of superseding entries
        """
        return self.__assembleRemovedData(self.__invD)

    def __assembleRemovedData(self, invD):
        """Assemble and reorganize removed data for RCSB loading operations.

        Args:
            invD (dict): json inventory of removed data

        Returns:
            dict: dictionary of transferred data,
            dict: dictionary of removed onsilico data
            dict: dictioary of audit authors of removed data
            dict: dictionary of all removed data
            dict: dictionary of superseding entries
        """
        # for transferred (which are the theoretical models, only pdb format is available):
        ct1MapD = {
            "pdb": "coordinates",
        }
        # for removed:
        ct2MapD = {
            "combined_nmr_data_nef": "Combined NMR data (NEF)",
            "combined_nmr_data_nmr-star": "Combined NMR data (NMR-STAR)",
            "nmr_chemical_shifts": "NMR chemical shifts",
            "nmr_restraints_v1": "NMR restraints V1",
            "nmr restraints v2": "NMR restraints V1",
            # "assembly PDB",
            # "assembly mmCIF",
            "pdb": "entry PDB",
            # "entry PDB bundle",
            "pdbml": "entry PDBML",
            "mmcif": "entry mmCIF",
            "structure_factors": "structure factors",
        }
        trMapD = {
            "deposition_authors": "audit_authors",
            "status_code": "status_code",
            "release_date": "release_date",
            "remote_repository_name": "remote_repository_name",
            "remote_repository_title": "title",
            "content_type": "repository_content_types",
            "deposit_date": "deposit_date",
            "remote_accession_code": "remote_accession_code",
            # "title": "title",
        }
        insMapD = {
            "deposition_authors": "audit_authors",
            "deposit_date": "deposit_date",
            "superseded_by": "id_codes_replaced_by",
            "release_date": "release_date",
            "obsolete_date": "remove_date",
            "status_code": "status_code",
            "title": "title",
        }
        removedMapD = {
            "deposition_authors": "audit_authors",
            "deposit_date": "deposit_date",
            "details": "details",
            "superseded_by": "id_codes_replaced_by",
            "release_date": "release_date",
            "obsolete_date": "remove_date",
            "content_type": "repository_content_types",
            "title": "title",
        }
        dateFields = ["deposit_date", "release_date", "remove_date"]
        trsfD = {}
        insilicoD = {}
        auditAuthorD = {}
        removedD = {}
        superD = {}
        # --- generate lookup for superseding entries ---
        replacedByD = {}
        replacesD = {}
        for entryId, tD in invD.items():
            if "superseded_by" in tD:
                if isinstance(tD["superseded_by"], str):
                    replacedByD[entryId] = [tD["superseded_by"].upper()]
                else:
                    replacedByD[entryId] = [t.upper() for t in tD["superseded_by"]]
        for entryId, rIdL in replacedByD.items():
            for rId in rIdL:
                replacesD.setdefault(rId, []).append(entryId)
        # ---
        for entryId, tD in invD.items():
            # --- Transferred ---
            if tD["status_code"] == "TRSF":
                qD = {}
                for iky, oky in trMapD.items():
                    if iky not in tD:
                        continue
                    if self.__assignDates and oky in dateFields:
                        qD[oky] = dateutil.parser.parse(tD[iky])
                    elif oky in dateFields:
                        qD[oky] = tD[iky][:10]
                    else:
                        qD[oky] = tD[iky]
                    #
                    if iky == "content_type":
                        cS = set()
                        for ct in tD["content_type"]:
                            if ct in ct1MapD:
                                cS.add(ct1MapD[ct])
                        qD["repository_content_types"] = sorted(list(cS))
                    #
                    if oky == "id_codes_replaced_by":
                        if isinstance(qD["id_codes_replaced_by"], str):
                            qD["id_codes_replaced_by"] = [qD["id_codes_replaced_by"].upper()]
                        else:
                            qD["id_codes_replaced_by"] = [t.upper() for t in qD["id_codes_replaced_by"]]
                trsfD[entryId] = qD
            else:
                # --- removed ---
                qD = {}
                for iky, oky in removedMapD.items():
                    if iky not in tD:
                        continue
                    if self.__assignDates and oky in dateFields:
                        qD[oky] = dateutil.parser.parse(tD[iky])
                    elif oky in dateFields:
                        qD[oky] = tD[iky][:10]
                    else:
                        qD[oky] = tD[iky]
                    #
                    if iky == "content_type":
                        cS = set()
                        for ct in tD["content_type"]:
                            if ct in ct2MapD:
                                cS.add(ct2MapD[ct])
                        qD["repository_content_types"] = sorted(list(cS))
                    #
                    if oky == "id_codes_replaced_by":
                        if isinstance(qD["id_codes_replaced_by"], str):
                            qD["id_codes_replaced_by"] = [qD["id_codes_replaced_by"].upper()]
                        else:
                            qD["id_codes_replaced_by"] = [t.upper() for t in qD["id_codes_replaced_by"]]
                    #
                removedD[entryId] = qD
            #
            # --- inslico models
            if tD["status_code"] in ["OBS", "TRSF", "WDRN"] and self.__isContentInsilico(tD["content_type"]):
                qD = {}
                for iky, oky in insMapD.items():
                    if iky not in tD:
                        continue
                    # yyyy-mm-dd
                    if self.__assignDates and oky in dateFields:
                        qD[oky] = dateutil.parser.parse(tD[iky])
                    elif oky in dateFields:
                        qD[oky] = tD[iky][:10]
                    else:
                        qD[oky] = tD[iky]
                    if oky == "id_codes_replaced_by":
                        if isinstance(qD["id_codes_replaced_by"], str):
                            qD["id_codes_replaced_by"] = [qD["id_codes_replaced_by"].upper()]
                        else:
                            qD["id_codes_replaced_by"] = [t.upper() for t in qD["id_codes_replaced_by"]]
                #
                insilicoD[entryId] = qD
            # --- audit authors ---
            if "deposition_authors" in tD:
                qL = []
                for ii, author in enumerate(tD["deposition_authors"], 1):
                    qL.append({"ordinal_id": ii, "audit_author": author})
                auditAuthorD[entryId] = qL

            #  ---- superseded ----
            if entryId in replacesD:
                superD[entryId] = {"id_codes_superseded": replacesD[entryId]}
        logger.info("# of transferred entries: %d", len(trsfD))
        logger.info("# of insilico entries: %d", len(insilicoD))
        logger.info("# of removed entries: %d", len(removedD))
        return trsfD, insilicoD, auditAuthorD, removedD, superD

    def __isContentInsilico(self, ctD):
        """Test if the content type dictionary contains an inslico model.

        Args:
            ctD (dict): content type dictionary

        Returns:
            (bool): True if content is insilico

        Example:

            "content_type": {
                    "pdb": [
                        "/pdb/data/structures/models/current/pdb/ir/pdb2ir4.ent.gz"
                    ]
                },
        """
        isModel = False
        for ct, pthL in ctD.items():
            if ct in ["pdb", "mmcif", "pdbml"]:
                for pth in pthL:
                    if pth.startswith("/pdb/data/structures/models"):
                        isModel = True
                        break
        return isModel
