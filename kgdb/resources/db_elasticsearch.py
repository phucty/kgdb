import http.client
import re
from collections import defaultdict

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, streaming_bulk
from tqdm import tqdm

from kgdb.config import config as cf
from kgdb.resources.db.db_entity_labels import DBELabel, norm_text
from kgdb.resources.db.utils import is_wikidata_item
from kgdb.utils import io_worker as iw

http.client._MAXLINE = 655360


class ESearch(object):
    def __init__(self):
        self.client = Elasticsearch(
            [{"host": "localhost", "port": 9200, "timeout": 300}]
        )

        self.index_en = cf.ES_INDEX_NAME_EN
        self.index_all = cf.ES_INDEX_NAME_ALL

    def build(self):
        status = self.ping()
        if not status:
            return

        self.get_indices()
        self.build_index(self.index_en)
        self.build_index(self.index_all)

    def ping(self):
        respond = self.client.ping()
        if respond:
            print("Elastic Search is running at localhost:9200")
            return True
        else:
            print("Could not connect Elastic Search")
            return False

    def get_indices(self):
        if not self.client.indices.exists(self.index_en):
            self.client.indices.create(index=self.index_en, body=cf.ES_MAPPING)
            iw.print_status(f"Create {self.index_en}")
        if not self.client.indices.exists(self.index_all):
            self.client.indices.create(index=self.index_all, body=cf.ES_MAPPING)
            iw.print_status(f"Create {self.index_all}")

    @staticmethod
    def gen_index_docs(iter_items, index_name):
        for label, label_id in iter_items:
            yield {"_op_type": "index", "_index": index_name, "label": label}

    def build_index(self, index_name, buff_size=100000, from_i=None):
        if from_i is None:
            from_i = 0
        db_labels = DBELabel()
        if index_name == cf.ES_INDEX_NAME_EN:
            iter_items = db_labels.iter_en(from_i)
            total = db_labels.size_labels_en()
        else:
            iter_items = db_labels.iter_all()
            total = db_labels.size_labels_all()
        obj_gen = self.gen_index_docs(iter_items, index_name)
        if from_i:
            total -= from_i

        for status, response in tqdm(
            streaming_bulk(self.client, chunk_size=buff_size, actions=obj_gen),
            total=total,
            desc=f"{index_name}",
        ):
            if not status:
                iw.print_status(response)

    def search_label(self, input_text, lang="en", fuzzy=False):
        input_text = norm_text(input_text, punctuations=False)

        def combine_result(is_fuzzy=False):
            res = defaultdict(float)
            index_name = self.index_en if lang == "en" else self.index_all
            try:
                if is_fuzzy:
                    q_text = {
                        "size": cf.LIMIT_SEARCH_ES,
                        "query": {"fuzzy": {"label": input_text}},
                    }
                else:
                    q_text = {
                        "size": cf.LIMIT_SEARCH_ES,
                        "query": {"match": {"label": input_text}},
                    }
                response = self.client.search(index=index_name, body=q_text)
            except Exception as message:
                iw.print_status(message)
                response = {}
            # _max_score = 0
            if response.get("hits", []):
                # if response["hits"].get("max_score"):
                #     _max_score = response["hits"]["max_score"]
                for hit in response["hits"].get("hits", []):
                    if not hit.get("_source"):
                        continue
                    res[hit["_source"]["label"]] = max(
                        res[hit["_source"]["label"]], hit["_score"]
                    )
            if res:
                min_scores = min(res.values())
                div_scores = max(res.values()) - min_scores
                if div_scores:
                    res = {k: ((v - min_scores) / div_scores) for k, v in res.items()}
                else:
                    res = {k: (v / min_scores) for k, v in res.items()}
            return res

        responds = combine_result(is_fuzzy=False)

        # responds = defaultdict(float)
        # for res_i, res_s in res_bm25.items():
        #     responds[res_i] += res_s
        if fuzzy:
            res_fuzzy = None
            try:
                res_fuzzy = combine_result(is_fuzzy=True)
            except Exception as message:
                iw.print_status(message, is_screen=False)
            if res_fuzzy:
                for res_i, res_s in res_fuzzy.items():
                    if responds.get(res_i):
                        responds[res_i] = max(res_s, responds[res_i])
                    else:
                        responds[res_i] = res_s
                    # responds = {k: v / 2. for k, v in responds.items()}
        if responds:
            max_score = max(responds.values())
        else:
            max_score = 0
        # if max_score:
        #     res = {k: (v / max_score) for k, v in res.items()}
        return responds, max_score

    def search_wd(self, input_text, limit=0, lang="en", fuzzy=False):
        if not input_text:
            return defaultdict(float)
        responds_label = defaultdict(float)
        max_score = 0
        is_wd_id = is_wikidata_item(input_text)
        if is_wd_id:
            responds_label = {input_text.upper(): 1}

        query_text = input_text

        if not responds_label:
            responds_label, max_score = self.search_label(
                query_text, lang=lang, fuzzy=fuzzy
            )

        if not responds_label:
            if "(" in query_text:
                new_query_string = re.sub(r"\((.*)\)", "", query_text).strip()
                if new_query_string != query_text:
                    responds_label, max_score = self.search_label(
                        new_query_string, lang=lang, fuzzy=fuzzy
                    )

            if "[" in query_text:
                new_query_string = re.sub(r"\([.*]\)", "", query_text).strip()
                if new_query_string != query_text:
                    responds_label, max_score = self.search_label(
                        new_query_string, lang=lang, fuzzy=fuzzy
                    )

        if '("' in input_text:
            new_query_string = re.search(r"\(\"(.*)\"\)", input_text)
            if new_query_string:
                new_query_string = new_query_string.group(1)
                if new_query_string != input_text:
                    extra, extra_max_score = self.search_label(
                        new_query_string, lang=lang, fuzzy=fuzzy
                    )
                    if extra:
                        for e_i, e_s in extra.items():
                            if not responds_label.get(e_i):
                                responds_label[e_i] = e_s

        if "[" in input_text:
            new_query_string = re.sub(r"\[(.*)\]", "", input_text)
            if new_query_string != input_text:
                extra, extra_max_score = self.search_label(
                    new_query_string, lang=lang, fuzzy=fuzzy
                )
                if extra:
                    responds_label = {k: v * 0.99 for k, v in responds_label.items()}
                    # responds_label_set = set(responds_label.keys())
                    for e_i, e_s in extra.items():
                        if responds_label.get(e_i):
                            responds_label[e_i] = max(e_s, responds_label[e_i])
                        else:
                            responds_label[e_i] = e_s
                        # if e_i not in responds_label_set:
                        #     responds_label[e_i] = e_s
                        #     responds_label_set.add(e_i)

        responds = responds_label
        # responds = defaultdict(float)
        # for r, r_s in responds_label.items():
        #     db_words = self.db_english if lang == "en" else self.db_multilingual
        #     respond_wds = db_words.get_words(r, page_rank=True)
        #     if not respond_wds:
        #         continue
        #     for res_wd, prank in respond_wds:
        #         if responds.get(res_wd):
        #             continue
        #         # prank = self.wiki_items.get_pagerank_score(res_wd)
        #         responds[res_wd] = r_s * cf.WEIGHT_ES + prank * cf.WEIGHT_PAGERANK

        if not responds:
            return []

        responds = sorted(responds.items(), key=lambda x: x[1], reverse=True)
        if not limit:
            limit = len(responds)

        if limit:
            responds = responds[:limit]
        return responds
