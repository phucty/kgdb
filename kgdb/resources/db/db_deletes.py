import gc
import os
import re
import string
import unicodedata
from collections import defaultdict
from enum import Enum
from itertools import combinations
from typing import List, Optional

import ftfy
from freaddb.db_lmdb import DBSpec, FReadDB, ToBytes
from pyroaring import BitMap
from tqdm import tqdm

from kgdb.config import config as cf
from kgdb.resources.db.db_dbpedia import DBDBpedia
from kgdb.resources.db.db_entity_labels import DBELabel
from kgdb.resources.db.db_wikidata import DBWikidata, is_wikidata_item
from kgdb.resources.db.db_wikipedia import DBWikipedia


class DB_DELETE_COLUMN_NAME(Enum):
    DELETE = "DELETE"


DB_E_LABEL_SCHEMA = {
    DB_DELETE_COLUMN_NAME.DELETE: DBSpec(
        DB_DELETE_COLUMN_NAME.DELETE.value, bytes_value=ToBytes.INT_BITMAP
    ),
}


def delete_edits_prefix(key, max_edit_dis, prefix_length, min_len=1):
    if len(key) > prefix_length:
        key = key[:prefix_length]
    _min_len = len(key) - max_edit_dis - 1
    if _min_len < min_len:
        _min_len = min_len - 1
    combine = {
        "".join(l) for i in range(_min_len, len(key)) for l in combinations(key, i + 1)
    }
    return combine


class DBDeletes(FReadDB):
    def __init__(
        self,
        lang: str = "en",
        prefix_len: int = 10,
        max_distance: int = 4,
        db_schema: Optional[List[DBSpec]] = DB_E_LABEL_SCHEMA.values(),
        read_only=True,
        buff_limit: int = cf.BUFF_LIMIT,
        create_new: bool = False,
        buff_deletes_limit=32_000_000,
    ):
        db_file = f"{cf.DIR_DATABASES}/{lang}_{max_distance}_{prefix_len}"
        super().__init__(db_file, db_schema, read_only, buff_limit, create_new)
        self.lang, self.max_distance, self.prefix_len = os.path.basename(db_file).split(
            "_"
        )
        self.max_distance = int(self.max_distance)
        self.prefix_len = int(self.prefix_len)
        self.buff_deletes = defaultdict(BitMap)
        self.buff_size_deletes = 0
        self.buff_limit_deletes = buff_deletes_limit

    def size(self):
        return self.get_column_size(DB_DELETE_COLUMN_NAME.DELETE.value) + len(
            self.buff_deletes
        )

    def save_deletes_buff(self):
        for k, v in tqdm(
            self.buff_deletes.items(), desc="Save buff", total=len(self.buff_deletes)
        ):
            self.merge_buff(DB_DELETE_COLUMN_NAME.DELETE.value, k, v)
        self.save_buff()
        self.buff_deletes.clear()
        self.buff_size_deletes = 0
        gc.collect()

    def add_deletes(self, term, label_id_list):
        n_deletes = delete_edits_prefix(term, self.max_distance, self.prefix_len)
        for delete in n_deletes:
            if not delete or set(delete) in {" ", ".", ",", ":"}:
                continue
            for label_id in label_id_list:
                self.buff_deletes[delete].add(label_id)
                self.buff_size_deletes += 1
            # "raso o" self._buff_deletes["raso o"].add(1)

            if self.buff_size_deletes > self.buff_limit_deletes:
                self.save_deletes_buff()

    def get_frequency(self, term: str):
        if not term:
            return 0
        results = self.get_value(
            DB_DELETE_COLUMN_NAME.DELETE.value,
            term[: self.prefix_len],
            get_memory_size=True,
        )
        if not results:
            return None
        return results

    def get_posting(self, term: str):
        if not term:
            return None
        results = self.get_value(
            DB_DELETE_COLUMN_NAME.DELETE.value,
            term[: self.prefix_len],
        )
        return results

    def build_from_labels(self, iter_obj, len_iter=0, step=1_000, from_i=0):
        def update_desc():
            return f"{self.lang}|{self.max_distance}|{self.prefix_len}| Deletes: {self.size():,} | buff: {self.buff_size_deletes / self.buff_limit_deletes * 100:.0f}%"

        if len_iter:
            p_bar = tqdm(desc=update_desc(), total=len_iter)
        else:
            p_bar = tqdm(desc=update_desc())

        label_pre = None
        label_id_pre = set()
        p_bar.update(from_i)
        for i, (label, label_id) in enumerate(iter_obj):
            if i < from_i:
                continue
            if i and i % step == 0:
                p_bar.update(step)
                p_bar.set_description(desc=update_desc())

            if not label or is_wikidata_item(label):
                continue

            label = label[: self.prefix_len]
            if label != label_pre:
                if label_id_pre:
                    self.add_deletes(label_pre, label_id_pre)
                label_pre = label
                label_id_pre = {label_id}
            else:
                label_id_pre.add(label_id)

        if label_id_pre:
            self.add_deletes(label_pre, label_id_pre)

        self.save_deletes_buff()
        self.save_buff()


def build_db_deletes(lang="en", prefix_len: int = 10, max_distance: int = 4, from_i=0):
    db = DBDeletes(
        read_only=False,
        prefix_len=prefix_len,
        lang=lang,
        max_distance=max_distance,
        buff_deletes_limit=150_000_000,
    )
    db.compact(update_db_size=False)
    # return
    db_labels = DBELabel()
    if lang == "en":
        iter_obj = db_labels.iter_en()
        len_iter = db_labels.size_labels_en()
    else:
        iter_obj = db_labels.iter_all()
        len_iter = db_labels.size_labels_all()

    db.build_from_labels(iter_obj=iter_obj, len_iter=len_iter, from_i=from_i)

    db.close()
    # Compact db
    db = DBDeletes(
        read_only=False, prefix_len=prefix_len, lang=lang, max_distance=max_distance
    )
