import gc
import re
import string
import unicodedata
from collections import defaultdict
from enum import Enum
from typing import List, Optional

import ftfy
from freaddb.db_lmdb import DBSpec, FReadDB, ToBytes
from pyroaring import BitMap
from tqdm import tqdm

from kgdb.config import config as cf
from kgdb.resources.db import db_rocks
from kgdb.resources.db.db_dbpedia import DBDBpedia
from kgdb.resources.db.db_wikidata import DBWikidata, is_wikidata_item
from kgdb.resources.db.db_wikipedia import DBWikipedia


class DB_E_LABEL_COLUMN_NAME(Enum):
    LABEL_LID = "LABEL_LID"
    LID_LABEL = "LID_LABEL"
    WDID_LABEL_EN = "WDID_LABEL_EN"
    WDID_LABEL_ALL = "WDID_LABEL_ALL"
    LID_WDID_LABEL_EN_RANK = "LID_WDID_LABEL_EN_RANK"
    LID_WDID_LABEL_ALL_RANK = "LID_WDID_LABEL_ALL_RANK"


DB_E_LABEL_SCHEMA = {
    DB_E_LABEL_COLUMN_NAME.LABEL_LID: DBSpec(DB_E_LABEL_COLUMN_NAME.LABEL_LID.value),
    DB_E_LABEL_COLUMN_NAME.LID_LABEL: DBSpec(
        DB_E_LABEL_COLUMN_NAME.LID_LABEL.value, integerkey=True
    ),
    DB_E_LABEL_COLUMN_NAME.WDID_LABEL_EN: DBSpec(
        DB_E_LABEL_COLUMN_NAME.WDID_LABEL_EN.value,
        integerkey=True,
        bytes_value=ToBytes.INT_NUMPY,
    ),
    DB_E_LABEL_COLUMN_NAME.WDID_LABEL_ALL: DBSpec(
        DB_E_LABEL_COLUMN_NAME.WDID_LABEL_ALL.value,
        integerkey=True,
        bytes_value=ToBytes.INT_NUMPY,
    ),
    DB_E_LABEL_COLUMN_NAME.LID_WDID_LABEL_EN_RANK: DBSpec(
        DB_E_LABEL_COLUMN_NAME.LID_WDID_LABEL_EN_RANK.value,
        integerkey=True,
    ),
    DB_E_LABEL_COLUMN_NAME.LID_WDID_LABEL_ALL_RANK: DBSpec(
        DB_E_LABEL_COLUMN_NAME.LID_WDID_LABEL_ALL_RANK.value, integerkey=True
    ),
}


def is_ascii(s) -> bool:
    try:
        s.encode(encoding="utf-8").decode("ascii")
    except UnicodeDecodeError:
        return False
    else:
        return True


def norm_text(
    text: str, punctuations: bool = False, article: bool = True, lower: bool = True
) -> str:
    text = ftfy.fix_text(text)
    text = "".join(filter(lambda c: unicodedata.category(c) != "Cf", text))
    text = unicodedata.normalize("NFKC", text)  # .replace("\u002D", "")
    if lower:
        text = text.lower()
        # text = re.sub(re.compile(r'\s+'), ' ', text)
    text.replace("(disambiguation)", "")
    # if not accents:
    #
    # remove accents: https://stackoverflow.com/a/518232
    # text = ''.join(c for c in unicodedata.normalize('NFD', text)
    #                if unicodedata.category(c) != 'Mn')
    # text = unicodedata.normalize('NFC', text)

    # Remove article
    if not article:
        text = re.sub(r"\b(a|an|the|and)\b", " ", text)

    # Remove 3 duplicate character
    # text = "".join(char if text.count(char, 0, i) < 2 else "-" for i, char in enumerate(text)))
    text = re.sub(r"([a-zA-Z])\1\1+", r"\1\1", text)

    # Remove punctuations
    if not punctuations:
        exclude_c = set(string.punctuation)
        tmp_text = "".join(c for c in text if c not in exclude_c)
        if tmp_text:
            text = tmp_text

    # Remove space, enter
    text = " ".join(text.split())
    return text


class DBELabel(FReadDB):
    def __init__(
        self,
        db_file: str = cf.DIR_DB_LABELS,
        db_schema: Optional[List[DBSpec]] = DB_E_LABEL_SCHEMA.values(),
        read_only=True,
        buff_limit: int = cf.BUFF_LIMIT,
        create_new: bool = False,
    ):
        super().__init__(db_file, db_schema, read_only, buff_limit, create_new)

        self._buff_vocab = defaultdict(int)
        self._len_vocab = self.size_vocab()

    def iter_en(self, from_i=0):
        i = -1
        for k, v in self.iter_db(DB_E_LABEL_COLUMN_NAME.LABEL_LID.value):
            if self.is_available(
                DB_E_LABEL_COLUMN_NAME.LID_WDID_LABEL_EN_RANK.value, v
            ):
                i += 1
                if i < from_i:
                    continue
                yield k, v

    def iter_all(self, from_i=0):
        for i, (k, v) in enumerate(
            self.iter_db(DB_E_LABEL_COLUMN_NAME.LABEL_LID.value)
        ):
            if i < from_i:
                continue
            yield k, v

    def size_vocab(self):
        return self.get_column_size(DB_E_LABEL_COLUMN_NAME.LID_LABEL.value)

    def size_labels_en(self):
        return self.get_column_size(DB_E_LABEL_COLUMN_NAME.LID_WDID_LABEL_EN_RANK.value)

    def size_labels_all(self):
        return self.get_column_size(
            DB_E_LABEL_COLUMN_NAME.LID_WDID_LABEL_ALL_RANK.value
        )

    def get_lid(self, label: str, add: bool = False, norm_label: bool = True):
        lid = None
        added = False
        if norm_label:
            labels_ = {
                norm_text(label, punctuations=True),
                norm_text(label, punctuations=False),
            }
        else:
            labels_ = {label}

        for label_ in labels_:
            if self._buff_vocab:
                lid = self._buff_vocab.get(label_)

            if lid is None:
                lid = self.get_value(DB_E_LABEL_COLUMN_NAME.LABEL_LID.value, label_)

        if lid is None and add:
            lid = self._len_vocab
            self._buff_vocab[label] = lid
            self._len_vocab += 1
            added = True
            # Add to rocks
            self.add_buff(DB_E_LABEL_COLUMN_NAME.LABEL_LID.value, label, lid)
            self.add_buff(DB_E_LABEL_COLUMN_NAME.LID_LABEL.value, lid, label)

        if self.buff_size == 0:
            del self._buff_vocab
            gc.collect()
            self._buff_vocab = defaultdict(int)
        if add:
            return lid, added
        else:
            return lid

    def get_labels_en_from_wd_id(self, wd_id, get_labels=True):
        label_lids = self.get_value(DB_E_LABEL_COLUMN_NAME.WDID_LABEL_EN.value, wd_id)
        if not get_labels:
            return label_lids
        return self.get_labels_from_lid_list(label_lids)

    def get_labels_all_from_wd_id(self, wd_id, get_labels=True):
        label_lids = self.get_value(DB_E_LABEL_COLUMN_NAME.WDID_LABEL_ALL.value, wd_id)
        if not get_labels:
            return label_lids
        return self.get_labels_from_lid_list(label_lids)

    def get_label_from_lid(self, lid: int):
        return self.get_value(DB_E_LABEL_COLUMN_NAME.LID_LABEL.value, lid)

    def get_labels_from_lid_list(self, lid_list: List[int]):
        return self.get_values(DB_E_LABEL_COLUMN_NAME.LID_LABEL.value, lid_list)

    def get_wd_ranking_from_label(
        self, label, lang: str = "en", search_objs: str = "entity"
    ):
        label_lid = self.get_lid(label)
        if label_lid is not None:
            if lang == "en":
                db_name = DB_E_LABEL_COLUMN_NAME.LID_WDID_LABEL_EN_RANK.value
            else:
                db_name = DB_E_LABEL_COLUMN_NAME.LID_WDID_LABEL_EN_RANK.value
            responds = self.get_value(db_name, label_lid)
            if not responds:
                return responds

            if search_objs == "entity":
                responds = responds[0]
            elif search_objs == "type":
                responds = responds[1]
            elif search_objs == "property":
                responds = responds[2]
            else:
                responds = {
                    "entity": responds[0],
                    "type": responds[1],
                    "property": responds[2],
                }
            return responds

        return None

    def _call_back_is_available(self, column_name: str, label: str):
        lid = self.get_lid(label)
        if lid is None:
            return False

        if self.is_available(column_name=column_name, key_obj=label):
            return True

        return False

    def is_available_en(self, label: str):
        return self._call_back_is_available(
            DB_E_LABEL_COLUMN_NAME.WDID_LABEL_EN.value, label
        )

    def is_available_all(self, label: str):
        return self._call_back_is_available(
            DB_E_LABEL_COLUMN_NAME.WDID_LABEL_ALL.value, label
        )

    def build_vocab(self, step: int = 1000):
        update_from_id = self.size_vocab()
        db_wd = DBWikidata()
        db_wp = DBWikipedia()
        db_dp = DBDBpedia()
        WD_ENTITY_NAME_PROPS = set(db_wd.get_lid_set(cf.WD_ENTITY_NAME_PROPS))
        n_en = 0

        def update_desc():
            return (
                f"vocab:{self._len_vocab:,}"
                f"|en:{n_en:,} ({self.buff_size / self.buff_limit * 100:.0f}%)"
            )

        p_bar = tqdm(desc=update_desc(), total=db_wd.size())
        for i, lid_wd in enumerate(db_wd.keys()):
            if i and i % step == 0:
                p_bar.update(step)
                p_bar.set_description(update_desc())
            labels_en, labels_all = set(), set()
            # Mapping
            wd_wikipedia = db_wd.get_wikipedia(lid_wd)
            wd_dbpedia = db_wd.get_dbpedia(lid_wd)

            # Extract entity labels
            # Wikidata
            tmp = db_wd.get_label(lid_wd)
            if tmp:
                labels_en.add(tmp)
                # Human name abbreviation
                lid_types = db_wd.get_instance_of(lid_wd)

                if lid_types and db_wd.get_lid("Q5") in lid_types:
                    name_abb = tmp.split()
                    if len(name_abb) > 1:
                        name_abb = name_abb[0][0] + ". " + " ".join(name_abb[1:])
                        labels_en.add(name_abb)

            # Labels
            tmp = db_wd.get_labels(lid_wd)
            if tmp:
                labels_all.update(tmp.values())
                tmp = tmp.get("en")
                if tmp:
                    labels_en.add(tmp)
            # Aliases
            tmp = db_wd.get_aliases(lid_wd)
            if tmp:
                for all_l in tmp.values():
                    labels_all.update(all_l)
                tmp = tmp.get("en")
                if tmp:
                    labels_en.update(tmp)
            # Other property
            wd_claims = db_wd.get_claims_literal(lid_wd)
            if wd_claims:
                for facts in wd_claims.values():
                    for prop_name, value_objs in facts.items():
                        if prop_name not in WD_ENTITY_NAME_PROPS:
                            continue
                        for wd_other_id in value_objs:
                            if wd_other_id.isdigit():
                                continue
                            if is_ascii(wd_other_id):
                                labels_en.add(wd_other_id)
                            else:
                                labels_all.add(wd_other_id)
            # Wikipedia
            if wd_wikipedia:
                labels_en.add(wd_wikipedia)
                tmp = db_wp.get_redirect_of(wd_wikipedia)
                if tmp:
                    labels_en.update(tmp)

            # DBpedia
            if wd_dbpedia:
                labels_en.add(wd_dbpedia)
                tmp = db_dp.get_redirect_of(wd_dbpedia)
                if tmp:
                    labels_en.update(tmp)

                tmp = db_dp.get_aliases_en(wd_dbpedia)
                if tmp:
                    labels_en.update(tmp)

                tmp = db_dp.get_aliases_all(wd_dbpedia)
                if tmp:
                    labels_all.update(tmp)

            # Post-processing
            labels_all.update(labels_en)
            lid_en, lid_all = set(), set()
            for label in labels_all:
                if is_wikidata_item(label):
                    continue
                labels_ = {
                    norm_text(label, punctuations=True),
                    norm_text(label, punctuations=False),
                }

                for label_ in labels_:
                    if not label_:
                        continue
                    lid_label, is_new = self.get_lid(label_, add=True, norm_label=False)
                    lid_all.add(lid_label)
                    if label in labels_en:
                        lid_en.add(lid_label)
                        if is_new:
                            n_en += 1

            if lid_all:
                self.add_buff(
                    DB_E_LABEL_COLUMN_NAME.WDID_LABEL_ALL.value, lid_wd, lid_all
                )
            if lid_en:
                self.add_buff(
                    DB_E_LABEL_COLUMN_NAME.WDID_LABEL_EN.value, lid_wd, lid_en
                )

        p_bar.close()
        return update_from_id

    def build_label_wd_id_ranking(self, input_column: str, output_column: str):
        ranking = defaultdict(BitMap)

        for k, values_list in tqdm(
            self.iter_db(input_column),
            total=self.get_column_size(input_column),
            desc="Build WD entity ranking",
        ):
            for v in values_list:
                ranking[v].add(k)

        for k, v in tqdm(ranking.items(), desc="Save ranking"):
            # serialize data here, we will do custom deserialize later
            self.add_buff(output_column, k, v.serialize())
        self.save_buff()

    def build_label_wd_id_ranking_pagerank(self, column_name, limit=1000):
        db_wikidata = DBWikidata()
        for k, v in tqdm(
            self.iter_db(column_name, deserialize_obj=False),
            total=self.get_column_size(column_name),
            desc="Build wd ranking with pagerank",
        ):
            v = BitMap.deserialize(v)
            # tmp_rank = {"qid_ent": {}, "qid_type": {}, "pid": {}}
            tmp_rank = [{}, {}, {}]

            for wd_lid in v:
                page_rank = db_wikidata.get_pagerank(wd_lid)
                page_rank = (page_rank - cf.WEIGHT_PR_MIN) / cf.WEIGHT_PR_DIV
                wd_qid = db_wikidata.get_qid(wd_lid)
                if wd_qid.startswith("P"):
                    tmp_rank[2][wd_lid] = page_rank
                elif wd_qid.startswith("Q"):
                    if db_wikidata.is_a_type(wd_lid):
                        tmp_rank[1][wd_lid] = page_rank
                    else:
                        tmp_rank[0][wd_lid] = page_rank

            tmp_rank = [
                sorted(rank_obj.items(), key=lambda x: x[1], reverse=True)[:limit]
                for rank_obj in tmp_rank
            ]
            self.add_buff(column_name, k, tmp_rank)
        self.save_buff()

    def build_ranking_list(self):
        self.build_label_wd_id_ranking(
            DB_E_LABEL_COLUMN_NAME.WDID_LABEL_EN.value,
            DB_E_LABEL_COLUMN_NAME.LID_WDID_LABEL_EN_RANK.value,
        )
        self.build_label_wd_id_ranking(
            DB_E_LABEL_COLUMN_NAME.WDID_LABEL_ALL.value,
            DB_E_LABEL_COLUMN_NAME.LID_WDID_LABEL_ALL_RANK.value,
        )

    def build_ranking_list_with_pagerank(self):
        self.build_label_wd_id_ranking_pagerank(
            DB_E_LABEL_COLUMN_NAME.LID_WDID_LABEL_EN_RANK.value
        )
        self.build_label_wd_id_ranking_pagerank(
            DB_E_LABEL_COLUMN_NAME.LID_WDID_LABEL_ALL_RANK.value
        )
