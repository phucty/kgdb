from __future__ import annotations

import bz2
import csv
import gzip
import os.path
import queue
import time
from collections import Counter, defaultdict
from enum import Enum
from typing import Any, Callable, List, Optional, Union

import marisa_trie
import numpy as np
import scipy
import scipy.sparse as sprs
import scipy.sparse.linalg
import scipy.spatial
import ujson
from freaddb.db_lmdb import DBSpec, FReadDB, ToBytes, serialize_key, serialize_value
from pyroaring import BitMap
from scipy import sparse
from tqdm import tqdm

from kgdb.config import config as cf
from kgdb.resources.db import db_dbpedia, db_wikipedia
from kgdb.resources.db.db_core import DBCore
from kgdb.resources.db.utils import is_wikidata_item
from kgdb.utils import io_worker as iw
from kgdb.utils.benchmark import profile


def compute_pagerank(
    graph, alpha=0.85, max_iter=1000, tol=1e-06, personalize=None, reverse=False
):
    if reverse:
        graph = graph.T
        iw.print_status("Reversed matrix")

    n, _ = graph.shape
    iw.print_status(f"Pagerank Calculation: {n} nodes")
    r = np.asarray(graph.sum(axis=1)).reshape(-1)

    k = r.nonzero()[0]

    D_1 = sprs.csr_matrix((1 / r[k], (k, k)), shape=(n, n))

    if personalize is None:
        personalize = np.ones(n)
    personalize = personalize.reshape(n, 1)
    s = (personalize / personalize.sum()) * n

    z_T = (((1 - alpha) * (r != 0) + (r == 0)) / n)[np.newaxis, :]
    W = alpha * graph.T @ D_1

    x = s
    oldx = np.zeros((n, 1))

    iteration = 0

    tmp_tol = scipy.linalg.norm(x - oldx)
    while tmp_tol > tol:
        iw.print_status(f"Iteration {iteration + 1} - Tol: {tmp_tol}")
        oldx = x
        x = W @ x + s @ (z_T @ x)
        iteration += 1
        if iteration >= max_iter:
            break
        tmp_tol = scipy.linalg.norm(x - oldx)
    x = x / sum(x)

    return x.reshape(-1)


def boolean_search(db, params, print_top=3, get_qid=True):
    start = time.time()
    wd_ids = db.get_haswbstatements(params, get_qid=get_qid)
    end = time.time() - start
    print("Query:")
    for logic, prop, qid in params:
        if prop is None:
            prop_label = ""
        else:
            prop_label = f" - {prop}[{db.get_label(prop)}]"

        print(f"{logic}{prop_label}- {qid}[{db.get_label(qid)}]")

    print(f"Answers: Found {len(wd_ids):,} items in {end:.5f}s")
    for i, wd_id in enumerate(wd_ids[:print_top]):
        if not get_qid:
            wd_id = db.get_qid(wd_id)
        print(f"{i + 1}. {wd_id} - {db.get_label(wd_id)}")
    print(f"{4}. ...")
    print()


def parse_sql_values(line):
    values = line[line.find("` VALUES ") + 9 :]
    latest_row = []
    reader = csv.reader(
        [values],
        delimiter=",",
        doublequote=False,
        escapechar="\\",
        quotechar="'",
        strict=True,
    )
    for reader_row in reader:
        for column in reader_row:
            if len(column) == 0 or column == "NULL":
                latest_row.append(chr(0))
                continue
            if column[0] == "(":
                new_row = False
                if len(latest_row) > 0:
                    if latest_row[-1][-1] == ")":
                        latest_row[-1] = latest_row[-1][:-1]
                        new_row = True
                if new_row:
                    yield latest_row
                    latest_row = []
                if len(latest_row) == 0:
                    column = column[1:]
            latest_row.append(column)
        if latest_row[-1][-2:] == ");":
            latest_row[-1] = latest_row[-1][:-2]
            yield latest_row


def reader_wikidata_dump(dir_dump):
    if ".bz2" in dir_dump:
        reader = bz2.BZ2File(dir_dump)
    elif ".gz" in dir_dump:
        reader = gzip.open(dir_dump, "rt")
    else:
        reader = open(dir_dump)

    if reader:
        for line in reader:
            yield line
        reader.close()


def parse_json_dump(json_line):
    if isinstance(json_line, bytes) or isinstance(json_line, bytearray):
        line = json_line.rstrip().decode(cf.ENCODING)
    else:
        line = json_line.rstrip()
    if line in ("[", "]"):
        return None

    if line[-1] == ",":
        line = line[:-1]
    try:
        obj = ujson.loads(line)
    except ValueError:
        return None
    if obj["type"] != "item" and is_wikidata_item(obj["id"]) is False:
        return None

    wd_id = obj["id"]
    wd_obj = {}

    def update_dict(attribute, attr_value):
        if attribute == "aliases":
            wd_obj[attribute] = {
                lang: {v.get(attr_value) for v in value}
                for lang, value in obj.get(attribute, {}).items()
            }

        else:
            wd_obj[attribute] = {
                lang: value.get(attr_value)
                for lang, value in obj.get(attribute, {}).items()
            }

    update_dict(attribute="labels", attr_value="value")
    update_dict(attribute="descriptions", attr_value="value")
    update_dict(attribute="sitelinks", attr_value="title")
    update_dict(attribute="aliases", attr_value="value")

    # Get english label:
    wd_obj["label"] = wd_obj.get("labels", {}).get("en", wd_id)

    # Statements
    if obj.get("claims"):
        for prop, claims in obj["claims"].items():
            if wd_obj.get("claims") is None:
                wd_obj["claims"] = defaultdict()

            # if wd_obj.get("claims_provenance") is None:
            #     wd_obj["claims_provenance"] = defaultdict()

            for claim in claims:
                if (
                    claim.get("mainsnak") is None
                    or claim["mainsnak"].get("datavalue") is None
                ):
                    continue
                claim_type = claim["mainsnak"]["datavalue"]["type"]
                claim_value = claim["mainsnak"]["datavalue"]["value"]

                claim_references = claim.get("references")
                if claim_references:
                    nodes = []
                    for reference_node in claim_references:
                        if not reference_node.get("snaks"):
                            continue
                        node = {}
                        for ref_prop, ref_claims in reference_node["snaks"].items():
                            for ref_claim in ref_claims:
                                if ref_claim.get("datavalue") is None:
                                    continue
                                ref_type = ref_claim["datavalue"]["type"]
                                ref_value = ref_claim["datavalue"]["value"]
                                if node.get(ref_type) is None:
                                    node[ref_type] = defaultdict(list)

                                if ref_type == "wikibase-entityid":
                                    ref_value = ref_value["id"]
                                elif ref_type == "time":
                                    ref_value = ref_value["time"]
                                    ref_value = ref_value.replace("T00:00:00Z", "")
                                    if ref_value[0] == "+":
                                        ref_value = ref_value[1:]
                                elif ref_type == "quantity":
                                    ref_unit = ref_value["unit"]
                                    ref_unit = ref_unit.replace(cf.WD, "")
                                    ref_value = ref_value["amount"]
                                    if ref_value[0] == "+":
                                        ref_value = ref_value[1:]
                                    ref_value = (ref_value, ref_unit)
                                elif ref_type == "monolingualtext":
                                    ref_value = ref_value["text"]

                                node[ref_type][ref_prop].append(ref_value)
                        nodes.append(node)
                    claim_references = nodes
                else:
                    claim_references = []

                if wd_obj["claims"].get(claim_type) is None:
                    wd_obj["claims"][claim_type] = defaultdict(list)

                # if wd_obj["claims_provenance"].get(claim_type) is None:
                #     wd_obj["claims_provenance"][claim_type] = defaultdict(list)

                if claim_type == "wikibase-entityid":
                    claim_value = claim_value["id"]
                elif claim_type == "time":
                    claim_value = claim_value["time"]
                    claim_value = claim_value.replace("T00:00:00Z", "")
                    if claim_value[0] == "+":
                        claim_value = claim_value[1:]
                elif claim_type == "quantity":
                    claim_unit = claim_value["unit"]
                    claim_unit = claim_unit.replace(cf.WD, "")
                    claim_value = claim_value["amount"]
                    if claim_value[0] == "+":
                        claim_value = claim_value[1:]
                    claim_value = (claim_value, claim_unit)
                elif claim_type == "monolingualtext":
                    claim_value = claim_value["text"]

                wd_obj["claims"][claim_type][prop].append(
                    {"value": claim_value, "references": claim_references}
                )
                # wd_obj["claims"][claim_type][prop].append(claim_value)
                # wd_obj["claims_provenance"][claim_type][prop].append(
                #     {"value": claim_value, "provenance": claim_references}
                # )

    return wd_id, wd_obj


class COLUMN(Enum):
    ID_LID = "ID_LID"
    LID_ID = "LID_ID"
    DBPEDIA = "DBPEDIA"
    WIKIPEDIA = "WIKIPEDIA"
    REDIRECT = "REDIRECT"
    REDIRECT_OF = "REDIRECT_OF"
    LABEL = "LABEL"
    LABELS = "LABELS"
    DESC = "DESC"
    ALIASES = "ALIASES"
    CLAIMS_ENT = "CLAIMS_ENT"
    CLAIMS_ENT_INV = "CLAIMS_ENT_INV"
    CLAIMS_LIT = "CLAIMS_LIT"
    SITELINKS = "SITELINKS"
    PAGERANK = "PAGERANK"


DBWD_SCHEMA = {
    COLUMN.ID_LID: DBSpec(COLUMN.ID_LID.value),
    COLUMN.LID_ID: DBSpec(COLUMN.LID_ID.value, integerkey=True),
    COLUMN.DBPEDIA: DBSpec(COLUMN.DBPEDIA.value, integerkey=True),
    COLUMN.WIKIPEDIA: DBSpec(COLUMN.WIKIPEDIA.value, integerkey=True),
    COLUMN.REDIRECT: DBSpec(COLUMN.REDIRECT.value, integerkey=True),
    COLUMN.REDIRECT_OF: DBSpec(
        COLUMN.REDIRECT_OF.value,
        integerkey=True,
        bytes_value=ToBytes.INT_NUMPY,
    ),
    COLUMN.LABEL: DBSpec(COLUMN.LABEL.value, integerkey=True),
    COLUMN.LABELS: DBSpec(COLUMN.LABELS.value, combinekey=True),
    COLUMN.DESC: DBSpec(COLUMN.DESC.value, combinekey=True),
    COLUMN.ALIASES: DBSpec(COLUMN.ALIASES.value, combinekey=True),
    COLUMN.SITELINKS: DBSpec(COLUMN.SITELINKS.value, combinekey=True),
    COLUMN.CLAIMS_ENT: DBSpec(
        COLUMN.CLAIMS_ENT.value, combinekey=True, bytes_value=ToBytes.INT_NUMPY
    ),
    COLUMN.CLAIMS_ENT_INV: DBSpec(
        COLUMN.CLAIMS_ENT_INV.value, combinekey=True, bytes_value=ToBytes.INT_BITMAP
    ),
    COLUMN.CLAIMS_LIT: DBSpec(COLUMN.CLAIMS_LIT.value, integerkey=True),
    COLUMN.PAGERANK: DBSpec(COLUMN.PAGERANK.value, integerkey=True),
}


class DBWikidata(DBCore):
    def __init__(
        self,
        db_file: str = cf.DIR_WIKIDB,
        db_schema: Optional[List[DBSpec]] = DBWD_SCHEMA.values(),
        readonly=True,
        buff_limit: int = cf.BUFF_LIMIT,
        map_size: int = cf.SIZE_1GB * 10,
        split_subdatabases: bool = True,
    ):
        super().__init__(
            db_file=db_file,
            db_schema=db_schema,
            readonly=readonly,
            buff_limit=buff_limit,
            map_size=map_size,
            split_subdatabases=split_subdatabases,
        )

    def build_redirects(
        self,
        dump_wd_page: str = cf.DIR_DUMP_WD_PAGE,
        dump_wd_redirect=cf.DIR_DUMP_WD_REDIRECT,
        step: int = 100_000,
    ):
        # self._build_lid()
        # self._build_redirects()

        # if not os.path.exists(cf.DIR_DUMP_WD_PAGE):
        #     raise Exception(f"Please download file {cf.DIR_DUMP_WD_PAGE}")

        # def update_desc():
        #     return (
        #         f"Wikidata pages | Buff: {self.buff_size / self.buff_limit * 100:.0f}%"
        #     )

        # wikidata_lid = {}
        # with gzip.open(dump_wd_page, "rt", encoding="utf-8", newline="\n") as f:
        #     p_bar = tqdm(desc=update_desc())
        #     i = 0
        #     for line in f:
        #         # if i >= 200000:
        #         # break
        #         if not line.startswith("INSERT INTO"):
        #             continue
        #         for v in parse_sql_values(line):
        #             if is_wikidata_item(v[2]):
        #                 i += 1
        #                 if i and i % step == 0:
        #                     p_bar.update(step)
        #                     p_bar.set_description(update_desc())
        #                 wikidata_lid[v[0]] = v[2]
        #     p_bar.update(i % step)
        #     p_bar.close()

        # if not os.path.exists(dump_wd_redirect):
        #     raise Exception(f"Please download file {dump_wd_redirect}")

        # redirect = {}
        # qid_set = set()
        # with gzip.open(dump_wd_redirect, "rt", encoding="utf-8", newline="\n") as f:
        #     i = 0
        #     p_bar = tqdm(desc="Wikidata redirects")
        #     for line in f:
        #         # if i > 200000:
        #         # break
        #         if not line.startswith("INSERT INTO"):
        #             continue
        #         for v in parse_sql_values(line):
        #             i += 1
        #             if is_wikidata_item(v[2]):
        #                 qid = wikidata_lid.get(v[0])
        #                 if not qid:
        #                     continue
        #                 qid_redirect = v[2]
        #                 redirect[qid] = qid_redirect
        #                 qid_set.update({qid, qid_redirect})
        #                 if i and i % step == 0:
        #                     p_bar.update(step)
        #     p_bar.update(i % step)
        #     p_bar.close()

        # # Create index value
        # qid_set.update(wikidata_lid.values())
        # del wikidata_lid

        # iw.save_obj_pkl(self.db_file + "_qid_set.pkl", qid_set)
        # iw.save_obj_pkl(self.db_file + "_redirect.pkl", redirect)
        # qid_set = iw.load_obj_pkl(self.db_file + "_qid_set.pkl")

        # qid_set = list(qid_set)
        # qid_set.sort()
        # for qid in tqdm(qid_set, total=len(qid_set), mininterval=2):
        #     self.get_lid(qid, create_new=True)
        # self.save_buff_lid()
        redirect = iw.load_obj_pkl(self.db_file + "_redirect.pkl")

        redirect_of = defaultdict(set)
        for k, v in tqdm(redirect.items(), total=len(redirect), mininterval=2):
            self.add_buff_with_lid(
                COLUMN.REDIRECT.value, k, v, encode_key=True, encode_value=True
            )
            redirect_of[v].add(k)
        del redirect

        for k, v in tqdm(redirect_of.items(), total=len(redirect_of), mininterval=2):
            self.add_buff_with_lid(
                COLUMN.REDIRECT_OF.value, k, v, encode_key=True, encode_value=True
            )
        self.save_buff_lid()

    def build_information(self):
        self.build_from_json_dump()
        self.build_haswbstatements()
        self.save_buff()
        self.compact()

    def build_mapping(self, step=10000):
        from kgdb.resources.db.db_dbpedia import DBDBpedia
        from kgdb.resources.db.db_wikipedia import DBWikipedia

        def add_mapping(other_db, column_name):
            for db_id, wd_id in tqdm(
                other_db.get_db_iter("WIKIDATA"),
                total=other_db.get_number_items_from("WIKIDATA"),
            ):
                if not wd_id:
                    continue
                db_id = other_db.get_id(db_id)
                self.add_buff_with_lid(column_name, wd_id, db_id, encode_key=True)

        wikipedia = DBWikipedia()
        dbpedia = DBDBpedia()
        add_mapping(other_db=wikipedia, column_name=COLUMN.WIKIPEDIA.value)
        add_mapping(other_db=dbpedia, column_name=COLUMN.DBPEDIA.value)
        # add mapping dbpedia (dbpedia -> wikipedia -> wikidata)

        for i, (dp_id, wd_id) in tqdm(
            enumerate(dbpedia.get_db_iter("WIKIPEDIA")),
            total=dbpedia.get_number_items_from("WIKIPEDIA"),
            mininterval=2,
        ):
            # if i and i % step == 0:
            # break
            wd_id = wikipedia.get_wikidata(wd_id)
            if not wd_id:
                continue
            dp_id = dbpedia.get_id(dp_id)
            if dp_id is None:
                continue
            self.add_buff_with_lid(COLUMN.DBPEDIA.value, wd_id, dp_id, encode_key=True)
        self.save_buff_lid()

    def _build_lid(self, dump_wd_page: str = cf.DIR_DUMP_WD_PAGE, step: int = 100_000):
        if not os.path.exists(cf.DIR_DUMP_WD_PAGE):
            raise Exception(f"Please download file {cf.DIR_DUMP_WD_PAGE}")

        def update_desc():
            return (
                f"Wikidata pages | Buff: {self.buff_size / self.buff_limit * 100:.0f}%"
            )

        with gzip.open(dump_wd_page, "rt", encoding="utf-8", newline="\n") as f:
            p_bar = tqdm(desc=update_desc())
            i = 0
            for line in f:
                if not line.startswith("INSERT INTO"):
                    continue
                for v in parse_sql_values(line):
                    if is_wikidata_item(v[2]):
                        i += 1
                        if i and i % step == 0:
                            p_bar.update(step)
                            p_bar.set_description(update_desc())
                        self.add_buff(
                            COLUMN.QID_LID.value,
                            v[2],
                            int(v[0]),
                            is_serialize_value=False,
                        )
                        self.add_buff(
                            COLUMN.LID_QID.value,
                            int(v[0]),
                            v[2],
                            is_serialize_value=False,
                        )
            p_bar.update(i % step)
            p_bar.close()
        self.save_buff()

    def _build_redirects(
        self,
        dump_wd_redirect=cf.DIR_DUMP_WD_REDIRECT,
        step=1_000,
    ):
        if not os.path.exists(dump_wd_redirect):
            raise Exception(f"Please download file {dump_wd_redirect}")

        redirect_of = defaultdict(set)
        with gzip.open(
            dump_wd_redirect,
            "rt",
            encoding="utf-8",
            newline="\n",
        ) as f:
            i = 0
            p_bar = tqdm(desc="Wikidata redirects")
            for line in f:
                if not line.startswith("INSERT INTO"):
                    continue
                for v in parse_sql_values(line):
                    if is_wikidata_item(v[2]):
                        lid = int(v[0])
                        lid_redirect = self.get_lid(v[2])
                        if lid_redirect is None:
                            continue
                        redirect_of[lid_redirect].add(lid)
                        self.add_buff(
                            COLUMN.REDIRECT.value,
                            lid,
                            lid_redirect,
                            is_serialize_value=False,
                        )
                        i += 1
                        if i and i % step == 0:
                            p_bar.update(step)
            p_bar.update(i % step)
            p_bar.close()
        for k, v in redirect_of.items():
            self.add_buff(COLUMN.REDIRECT_OF.value, k, v)
        self.save_buff()

    def build_from_json_dump(self, json_dump=cf.DIR_DUMP_WD_JSON, step=1_000):
        column_name = {
            "label": COLUMN.LABEL.name,
            "labels": COLUMN.LABELS.name,
            "descriptions": COLUMN.DESC.name,
            "aliases": COLUMN.ALIASES.name,
            "claims": COLUMN.CLAIMS_LIT.name,
            "claims_ent": COLUMN.CLAIMS_ENT.name,
            "sitelinks": COLUMN.SITELINKS.name,
        }
        self.buff_limit = cf.SIZE_1GB * 10
        count = 0

        def update_desc():
            return f"Wikidata Parsing | items:{count:,} | buff: {self.buff_size / self.buff_limit * 100:.0f}%"

        p_bar = tqdm(
            desc=update_desc(),
            total=self.get_column_size(COLUMN.LID_QID.value),
        )
        for i, iter_item in enumerate(reader_wikidata_dump(json_dump)):
            wd_respond = parse_json_dump(iter_item)
            # if count > 1000:
            #     break
            if i and i % step == 0:
                p_bar.set_description(desc=update_desc())
                p_bar.update(step)
            if not wd_respond:
                continue
            wd_id, wd_obj = wd_respond
            lid = self.get_lid(wd_id)
            if lid is None:
                continue
            is_redirect = self.get_redirect(lid)
            if is_redirect is not None:
                continue

            if wd_obj.get("claims") and wd_obj["claims"].get("wikibase-entityid"):
                if wd_obj["claims"]["wikibase-entityid"].get("P31"):
                    instance_ofs = {
                        i["value"] for i in wd_obj["claims"]["wikibase-entityid"]["P31"]
                    }
                    if cf.WIKIDATA_IDENTIFIERS.intersection(instance_ofs):
                        continue
                if wd_obj["claims"]["wikibase-entityid"].get("P279"):
                    subclass_ofs = {
                        i["value"]
                        for i in wd_obj["claims"]["wikibase-entityid"]["P279"]
                    }
                    if cf.WIKIDATA_IDENTIFIERS.intersection(subclass_ofs):
                        continue
            count += 1

            for attr, value in wd_obj.items():
                if not value:
                    continue

                if attr == "claims":
                    lid_attr = {}
                    for c_type, c_statements in value.items():
                        lid_c_type = {}
                        for c_prop, c_values in c_statements.items():
                            lid_c_prop = self.get_lid(c_prop, redirect=True)
                            if lid_c_prop is None:
                                continue
                            lid_c_values = []
                            for c_value in c_values:
                                lid_c_value = c_value["value"]
                                if c_type == "wikibase-entityid":
                                    lid_c_value = self.get_lid(
                                        lid_c_value, redirect=True
                                    )
                                    if lid_c_value is None:
                                        continue
                                elif c_type == "quantity":
                                    if lid_c_value[1] != "1":
                                        lid_unit = self.get_lid(
                                            lid_c_value[1], redirect=True
                                        )
                                        if lid_unit is None:
                                            lid_unit = lid_c_value[1]
                                        lid_c_value = (
                                            lid_c_value[0],
                                            lid_unit,
                                        )
                                    else:
                                        lid_c_value = (lid_c_value[0], -1)
                                lid_c_values.append(lid_c_value)

                            lid_c_type[lid_c_prop] = lid_c_values

                        lid_attr[c_type] = lid_c_type
                    value = lid_attr
                    if value.get("wikibase-entityid"):
                        for prop_lid, values_ent in value["wikibase-entityid"].items():
                            if isinstance(prop_lid, int) and not any(
                                v for v in values_ent if not isinstance(v, int)
                            ):
                                key_ent = [lid, prop_lid]
                                self.add_buff(
                                    column_name["claims_ent"],
                                    key_ent,
                                    values_ent,
                                    is_serialize_value=False,
                                )
                        del value["wikibase-entityid"]
                self.add_buff(column_name[attr], lid, value)
        self.save_buff()

    def build_haswbstatements(self, step: int = 1_000_000):
        print("Build haswbstatements")
        invert_index = defaultdict(BitMap)
        for i, (key, value) in enumerate(
            tqdm(
                self.get_db_iter(COLUMN.CLAIMS_ENT.value),
                total=self.get_column_size(COLUMN.CLAIMS_ENT.value),
            )
        ):
            # if i and i % step == 0:
            #     break
            head_lid, prop_lid = key
            for v in value:
                invert_index[(v, prop_lid)].add(head_lid)

        invert_index = sorted(invert_index.items(), key=lambda x: x[0])

        tail_kv_list = []
        tail_k = None
        tail_v = BitMap()
        for k, v in tqdm(invert_index, desc="Save db", total=len(invert_index)):
            tmp_k = k[0]
            if tmp_k != tail_k:
                if tail_k:
                    tail_k = serialize_key([tail_k], combinekey=True)
                    tail_v = serialize_value(tail_v, bytes_value=ToBytes.INT_BITMAP)
                    self.add_buff(
                        COLUMN.CLAIMS_ENT_INV.value,
                        tail_k,
                        tail_v,
                        is_serialize_value=False,
                    )
                    for k_, v_ in tail_kv_list:
                        self.add_buff(
                            COLUMN.CLAIMS_ENT_INV.value,
                            k_,
                            v_,
                            is_serialize_value=False,
                        )

                tail_k = tmp_k
                tail_v = BitMap()
                tail_kv_list = []

            tail_v.update(v)

            k = serialize_key(k, combinekey=True)
            v = serialize_value(v, bytes_value=ToBytes.INT_BITMAP)
            tail_kv_list.append((k, v))

        self.save_buff()

    def build_db_pagerank(
        self,
        n_cpu=1,
        alpha=0.85,
        max_iter=1000,
        tol=1e-06,
        personalize=None,
        reverse=True,
        step=10000,
    ):
        from kgdb.resources.db.db_dbpedia import DBDP
        from kgdb.resources.db.db_wikipedia import DBWP

        dbpedia = DBDP()
        wikipedia = DBWP()
        row, col, data = [], [], []
        # keys = []
        # for i, k in enumerate(self.keys()):
        #     if i >= 100000:
        #         break
        #     keys.append(k)
        keys = [k for k in self.keys()]
        keys.sort()

        for wd_lid in tqdm(keys, total=len(keys)):
            outlinks = Counter()
            # All Wikidata claims
            wd_claims = self.get_claims_entity(wd_lid)
            if wd_claims:
                for _, wd_values in wd_claims.items():
                    for wd_value in wd_values:
                        outlinks[wd_value] += cf.WEIGHT_WD

            wp_id = self.get_wikipedia(wd_lid)
            if wp_id:
                wp_obj = wikipedia.get_item(wp_id)
                if wp_obj and wp_obj["claims_wd"]:
                    for wd_prop, wp_entities in wp_obj["claims_wd"].items():
                        if "Section" in wd_prop:
                            weight_prop = cf.WEIGHT_W_OTHERS
                        else:
                            weight_prop = cf.WEIGHT_WD
                        for wp_entity in wp_entities:
                            map_wd_id = wikipedia.get_wikidata(wp_entity)
                            if not map_wd_id or not is_wikidata_item(map_wd_id):
                                continue
                            redirect_wd = self.get_redirect(map_wd_id)
                            if redirect_wd:
                                map_wd_id = redirect_wd
                            if isinstance(map_wd_id, str):
                                map_wd_id = self.get_lid(map_wd_id)
                            if map_wd_id is not None:
                                outlinks[map_wd_id] += weight_prop

            dp_id = self.get_dbpedia(wd_lid)
            if dp_id:
                dp_obj = dbpedia.get_claims_entity(dp_id)
                if dp_obj:
                    for dp_entities in dp_obj.values():
                        for dp_entity in dp_entities:
                            map_wd_id = dbpedia.get_wikidata(dp_entity)
                            if not map_wd_id:
                                map_wd_id = dbpedia.get_wikipedia(dp_entity)
                                if map_wd_id:
                                    map_wd_id = wikipedia.get_wikidata(map_wd_id)

                            if not map_wd_id or not is_wikidata_item(map_wd_id):
                                continue

                            redirect_wd = self.get_redirect(map_wd_id)
                            if redirect_wd:
                                map_wd_id = redirect_wd
                            if isinstance(map_wd_id, str):
                                map_wd_id = self.get_lid(map_wd_id)
                            if map_wd_id is not None:
                                outlinks[map_wd_id] += cf.WEIGHT_WD

            if not outlinks:
                continue

            for tail_id, tail_weights in outlinks.items():
                row.append(int(wd_lid))
                col.append(int(tail_id))
                data.append(int(tail_weights))

        n = max(max(row), max(col)) + 1
        # # row, col, data = zip(*sorted(zip(row, col, data)))
        iw.save_obj_pkl(
            f"{cf.DIR_DATABASES}/row_col_data.pkl",
            {"row": row, "col": col, "data": data},
        )
        graph = sparse.csr_matrix((data, (row, col)), dtype=np.uintc, shape=(n, n))
        del data
        del row
        del col
        pagerank = compute_pagerank(graph, alpha, max_iter, tol, personalize, reverse)

        # save pagerank stats for normalization later
        pagerank = np.array(pagerank)
        pagerank_stats = {
            "max": np.max(pagerank),
            "min": np.min(pagerank),
            "std": np.std(pagerank),
            "mean": np.mean(pagerank),
            "div": np.max(pagerank) - np.min(pagerank),
        }
        iw.save_obj_pkl(cf.DIR_WIKI_PAGERANK_STATS, pagerank_stats)
        iw.print_status(pagerank_stats)
        iw.save_obj_pkl(cf.DIR_WIKI_GRAPH_PAGERANK, pagerank)
        for i, score in tqdm(enumerate(pagerank), desc="Saving"):
            if self.is_available(COLUMN.LID_QID.value, i):
                self.add_buff(COLUMN.PAGERANK.value, i, score)
        self.save_buff()

    def get_items_info(self, item_ids: Any, lang: str = "en"):
        responds_info = []
        for item_id in item_ids:
            responds_obj = defaultdict()
            if isinstance(item_id, tuple) or isinstance(item_id, list):
                item_id, item_score = item_id
                responds_obj["score"] = item_score

            if isinstance(item_id, int):
                qid = self.get_qid(item_id)
            else:
                qid = item_id
                item_id = self.get_lid(item_id)

            responds_obj["id"] = qid

            if qid[0] == "Q":
                responds_obj["wikidata"] = cf.WD + qid
            elif qid[0] == "P":
                responds_obj["wikidata"] = cf.WDT + qid
            else:
                raise ValueError("Not found wikidata item")
            responds_obj["label"] = self.get_label(item_id)
            # responds_obj["label"] = self.get_labels(item_id, lang=lang)
            # responds_obj["description"] = self.get_desc(item_id, lang=lang)

            # wp = self.get_wikipedia_title(item_id, lang=lang)
            # if wp:
            #     responds_obj["wikipedia"] = cf.WIKI_EN + wp.replace(" ", "_")
            #
            # dp = self.get_dbpedia(item_id)
            # if dp:
            #     responds_obj["dbpedia"] = cf.DBR + dp.replace(" ", "_")
            responds_info.append(responds_obj)
        return responds_info

    def _call_back_get_item_with_redirect(
        self,
        func: Callable,
        column_name: str,
        key: Union[str, int],
        redirect: bool = False,
    ):
        if redirect:
            key_redirect = self.get_redirect(key)
            if key_redirect is not None:
                key = key_redirect

        result = func(column_name, key)
        return result

    def _call_back_get_items_with_redirects(
        self,
        func: Callable,
        column_name: str,
        keys: List[Union[str, int]],
        redirect: bool = False,
    ):
        if redirect and keys:
            keys_redirect = set()
            for key in keys:
                key_redirect = self.get_redirect(key)
                if key_redirect is None:
                    keys_redirect.add(key)
                else:
                    keys_redirect.add(key_redirect)
            keys = keys_redirect
        results = func(column_name, keys, get_values_only=True)
        return results

    def _call_back_get_item_with_lid_qid(
        self,
        func: Callable,
        column_name: str,
        item_id: Union[str, int],
        decode_value: bool = False,
    ):
        if isinstance(item_id, str):
            item_id = self.get_lid(item_id)

        if item_id is None:
            return None

        results = func(column_name, item_id)
        if not decode_value:
            return results

        if decode_value:
            if isinstance(results, int):
                return self.get_qid(results)
            if (
                isinstance(results, list)
                or isinstance(results, np.ndarray)
                or isinstance(results, BitMap)
            ):
                return [self.wikidata_trie.restore_key(r) for r in results]
        return None

    def _call_back_get_item_with_lid_qid_lang(
        self,
        func: Callable,
        column_name: str,
        item_id: Union[str, int],
        lang: Optional[str] = None,
    ):
        result = self._call_back_get_item_with_lid_qid(func, column_name, item_id)
        if not result:
            return None
        if lang:
            return result.get(lang)
        return result

    def get_wikipedia(self, item_id: Union[str, int]):
        return self._call_back_get_item_with_lid_qid(
            func=self.get_value,
            column_name=COLUMN.WIKIPEDIA.value,
            item_id=item_id,
        )

    def get_dbpedia(self, item_id: Union[str, int]):
        return self._call_back_get_item_with_lid_qid(
            func=self.get_value,
            column_name=COLUMN.DBPEDIA.value,
            item_id=item_id,
        )

    def get_label(self, item_id: Union[str, int]):
        return self._call_back_get_item_with_lid_qid(
            func=self.get_value,
            column_name=COLUMN.LABEL.value,
            item_id=item_id,
        )

    def get_pagerank(self, item_id: Union[str, int]):
        return self._call_back_get_item_with_lid_qid(
            func=self.get_value,
            column_name=COLUMN.PAGERANK.value,
            item_id=item_id,
        )

    def get_labels(self, item_id: Union[str, int], lang: Optional[str] = None):
        return self._call_back_get_item_with_lid_qid_lang(
            func=self.get_value,
            column_name=COLUMN.LABELS.value,
            item_id=item_id,
            lang=lang,
        )

    def get_desc(self, item_id: Union[str, int], lang: Optional[str] = None):
        return self._call_back_get_item_with_lid_qid_lang(
            func=self.get_value,
            column_name=COLUMN.DESC.value,
            item_id=item_id,
            lang=lang,
        )

    def get_aliases(self, item_id: Union[str, int], lang: Optional[str] = None):
        return self._call_back_get_item_with_lid_qid_lang(
            func=self.get_value,
            column_name=COLUMN.ALIASES.value,
            item_id=item_id,
            lang=lang,
        )

    def get_sitelinks(self, item_id: Union[str, int], site: Optional[str] = None):
        return self._call_back_get_item_with_lid_qid_lang(
            func=self.get_value,
            column_name=COLUMN.SITELINKS.value,
            item_id=item_id,
            lang=site,
        )

    def get_claims_literal(
        self, item_id: Union[str, int], datatype: str = None, get_qid: bool = False
    ):
        literals = self._call_back_get_item_with_lid_qid_lang(
            func=self.get_value,
            column_name=COLUMN.CLAIMS_LIT.value,
            item_id=item_id,
            lang=datatype,
        )

        if not get_qid:
            return literals

        if datatype:
            literals = {datatype: literals}
        results = {}
        for dt, dt_objs in literals.items():
            if results.get(dt) is None:
                results[dt] = {}
            for prop, values in dt_objs.items():
                prop_qid = self.get_qid(prop)
                values_qid = []
                for value in values:
                    if dt == "quantity":
                        if isinstance(value[1], int) and value[1] >= 0:
                            unit_qid = self.get_qid(value[1])
                            if unit_qid:
                                value[1] = unit_qid
                    values_qid.append(value)
                results[dt][prop_qid] = values_qid

        if datatype:
            return list(results.values())
        return results

    def get_wikipedia_title(self, item_id: Union[str, int], lang: str):
        site_lang = f"{lang}wiki"
        result = self.get_sitelinks(item_id=item_id, site=site_lang)
        return result

    def get_wikipedia_link(self, item_id: Union[str, int], lang: str):
        title = self.get_wikipedia_title(lang=lang, item_id=item_id)
        if title and isinstance(title, str):
            title = title.replace(" ", "_")
            return f"https://{lang}.wikipedia.org/wiki/{title}"
        return None

    def get_instance_of(self, item_id: Union[str, int], get_qid: bool = False):
        return self.get_claims_entity(item_id=item_id, prop_id="P31", get_qid=get_qid)

    def get_subclass_of(self, item_id: Union[str, int], get_qid: bool = False):
        return self.get_claims_entity(item_id=item_id, prop_id="P279", get_qid=get_qid)

    def get_all_types(self, item_id: Union[str, int], get_qid: bool = False):
        # wdt:P31/wdt:P279*
        results = set()
        p_items = self.get_instance_of(item_id=item_id, get_qid=get_qid)
        if p_items:
            process_queue = queue.Queue()
            for p_item in p_items:
                process_queue.put(p_item)
            while process_queue.qsize():
                process_wd = process_queue.get()
                results.add(process_wd)
                p_items = self.get_subclass_of(item_id=process_wd, get_qid=get_qid)
                if p_items:
                    for item in p_items:
                        if item not in results:
                            process_queue.put(item)
        return list(results)

    def get_claims_entity(
        self,
        item_id: Union[str, int],
        prop_id: Optional[Any] = None,
        get_qid: bool = False,
    ):
        if isinstance(item_id, str):
            item_id = self.get_lid(item_id)

        if item_id is None:
            return None

        if prop_id is not None:
            if not isinstance(prop_id, int):
                prop_id = self.get_lid(prop_id)
                if prop_id is None:
                    return None

            key = serialize_key([item_id, prop_id], combinekey=True)
            results = self.get_value(COLUMN.CLAIMS_ENT.value, key, to_list=True)
            if results and get_qid:
                results = self.get_qid_set(results)
        else:
            results = {}
            key_prefix = serialize_key(
                [item_id], combinekey=True, get_postfix_deliminator=True
            )
            for key, value in self.iter_db_prefix(
                COLUMN.CLAIMS_ENT.value, key_prefix, to_list=True
            ):
                if get_qid:
                    key = tuple(self.get_qid(k) for k in key)
                    value = self.get_qid_set(value)
                results[key] = value
        return results

    def get_item(self, item_id: Union[str, int], get_qid: bool = False):
        if isinstance(item_id, str):
            item_id = self.get_lid(item_id)

        if item_id is None:
            return None

        result = dict()
        result["wikidata_id"] = self.get_qid(item_id, redirect=True)

        def update_dict(attr, func):
            if attr in {"claims_literal", "claims_entity"}:
                tmp = func(item_id, get_qid=get_qid)
            else:
                tmp = func(item_id)
            if tmp is not None:
                result[attr] = tmp

        update_dict("label", self.get_label)
        update_dict("labels", self.get_labels)
        update_dict("descriptions", self.get_desc)
        update_dict("aliases", self.get_aliases)
        update_dict("sitelinks", self.get_sitelinks)

        update_dict("claims_literal", self.get_claims_literal)
        update_dict("claims_entity", self.get_claims_entity)
        return result

    def size(self):
        return self.get_column_size(COLUMN.LID_QID.value)

    def is_a_type(self, wd_id):
        if not isinstance(wd_id, int):
            wd_id = self.get_lid(wd_id)
            if wd_id is None:
                return None
        property_id = self.get_lid("P279")
        return self.is_available(COLUMN.CLAIMS_ENT_INV.value, (wd_id, property_id))

    def get_haswbstatements(self, statements, get_qid=True, show_progress=False):
        results = None
        # sort attr
        log_message = []
        if statements:
            sorted_attr = []
            for operation, pid, qid in statements:
                fre = None
                if pid and qid:
                    fre = self.get_qid_another_side(
                        COLUMN.CLAIMS_ENT_INV.value,
                        qid,
                        pid,
                        get_memory_size=True,
                        get_qid=False,
                    )
                elif qid:
                    fre = self.get_qid_another_side(
                        COLUMN.CLAIMS_ENT_INV.value,
                        qid,
                        get_memory_size=True,
                        get_qid=False,
                    )
                if fre is None:
                    continue
                sorted_attr.append([operation, pid, qid, fre])
            sorted_attr.sort(key=lambda x: x[3])
            statements = [
                [operation, pid, qid] for operation, pid, qid, f in sorted_attr
            ]

        for operation, pid, qid in statements:
            if pid and qid:
                tmp = self.get_qid_another_side(
                    COLUMN.CLAIMS_ENT_INV.value, qid, pid, get_qid=False
                )
            elif qid:
                tmp = self.get_qid_another_side(
                    COLUMN.CLAIMS_ENT_INV.value, qid, get_qid=False
                )
            else:
                tmp = BitMap()

            if results is None:
                results = tmp
                if tmp is None:
                    break
            else:
                if operation == cf.ATTR_OPTS.AND:
                    results = results & tmp
                elif operation == cf.ATTR_OPTS.OR:
                    results = results | tmp
                elif operation == cf.ATTR_OPTS.NOT:
                    results = BitMap.difference(results, tmp)
                else:  # default = AND
                    results = results & tmp
            log_message.append(
                f"  {operation}. {pid}={qid} ({self.get_label(pid)}={self.get_label(qid)}) : {len(tmp):,} --> Context: {len(results):,}"
            )
        if not results:
            return []
        if get_qid:
            results = list(self.get_qid_set(results))
        else:
            results = results.to_array()
        if show_progress:
            print("\n".join(log_message))
        return results

    def get_qid_another_side(
        self,
        column_name: str,
        qid_one_side: Union[str, int],
        pid: Optional[Union[str, int]] = None,
        get_memory_size: bool = False,
        get_qid: bool = False,
    ):
        if not isinstance(qid_one_side, int):
            qid_one_side = self.get_lid(qid_one_side)
            if qid_one_side is None:
                return None

        if pid and not isinstance(pid, int):
            pid = self.get_lid(pid)
            if pid is None:
                return None

        if not pid:
            key = [qid_one_side]
        else:
            key = [qid_one_side, pid]

        if get_memory_size:
            return self.get_value(column_name, key, get_memory_size=True)

        posting = self.get_value(column_name, key)
        if get_qid:
            posting = self.get_qid_set(posting)
        return posting


if __name__ == "__main__":
    db = DBWikidata()
    tmp = 1
    db.get_item()
