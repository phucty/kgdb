from collections import defaultdict
from time import time

from kgdb.m_f import init
from kgdb.resources.db.db_entity_labels import DBELabel
from kgdb.resources.db.db_wikidata import DBWikidata
from kgdb.resources.db.utils import is_wikidata_item
from kgdb.resources.db_elasticsearch import ESearch
from kgdb.utils import io_worker as iw
from kgdb.utils import similarities
from kgdb.utils import utils as ul
from kgdb.utils.benchmark import profile

wiki_items = DBWikidata()
wiki_labels = DBELabel()
search_e = ESearch()


def search(
    query, attr=None, lang="en", mode="a", limit=20, expensive=False, fuzzy=False
):
    # One search query will create new instances search_e, search_f, and wiki_items

    search_f = None

    # Check query is a wikidata item or not
    is_wd_id = is_wikidata_item(query)

    # Check is query multilingual
    # query_lang = m_f.lang_pre().predict(query)
    # if query_lang != "en":
    #     lang = "all"
    if not ul.isEnglish(query):
        lang = "all"

    # iw.print_status(f"Lang: {lang}")

    # Attribute handling
    if attr:
        # Todo: Implement entity search
        pass

    if is_wd_id:
        return [[query.upper(), 1]]

    if mode == "b":
        responds_label = search_e.search_wd(query, limit=limit, lang=lang, fuzzy=fuzzy)
    elif mode == "f":
        responds_label = search_f.search_wd(
            query, limit=limit, lang=lang, expensive=expensive
        )
    else:
        responds_label_e = search_e.search_wd(
            query, limit=limit, lang=lang, fuzzy=fuzzy
        )
        responds_label_f = search_f.search_wd(
            query, limit=limit, lang=lang, expensive=expensive
        )
        responds_label = ul.merge_ranking(
            [responds_label_e, responds_label_f], weight=[0.9, 1], is_sorted=True
        )

    if not responds_label:
        return []

    responds = defaultdict(float)

    for _r_i, (_respond, _res_s) in enumerate(responds_label):
        respond_wds = wiki_labels.get_wd_ranking_from_label(
            _respond, lang, search_objs="entity"
        )
        if not respond_wds:
            continue

        for _res_wd, _prank in respond_wds:
            if lang == "en":
                # _responds[_res_wd] = max(_responds[_res_wd], _res_s * 0.7 + _prank * 0.3)

                main_label = wiki_items.get_label(_res_wd)
                main_label_sim = 0
                if main_label:
                    main_label_sim = similarities.sim_fuzz_ratio(main_label, query)

                label_en = wiki_labels.get_labels_en_from_wd_id(_res_wd)
                label_en_sim = 0
                if label_en:
                    label_en_closest = similarities.get_closest(query, label_en)
                    if label_en_closest:
                        c_label, label_en_sim = label_en_closest

                responds[_res_wd] = max(
                    responds[_res_wd],
                    _res_s * 0.4
                    + _prank * 0.3
                    + main_label_sim * 0.001
                    + label_en_sim * 0.3,
                )
            else:
                label_all = wiki_labels.get_labels_all_from_wd_id(_res_wd)
                label_all_sim = 0
                if label_all:
                    label_all_closest = similarities.get_closest(query, label_all)
                    if label_all_closest:
                        c_label, label_all_sim = label_all_closest

                responds[_res_wd] = max(
                    responds[_res_wd],
                    _res_s * 0.4 + _prank * 0.3 + label_all_sim * 0.3,
                )
            # if limit and len(_responds) > limit:
            #     break
        if limit and len(responds) > limit:
            break

    if limit == 0:
        limit = len(responds)
    responds = sorted(responds.items(), key=lambda x: x[1], reverse=True)[:limit]
    return responds


@profile
def run_entity_search(lang="en", mode="a", limit=20, expensive=False):
    iw.print_status(f"\nlang={lang} - mode={mode}----------------------------")
    # wiki_items = DBWD()
    queries = [
        "* TM-88",
        "2MASS J0343118+6137172",
        "2MASS J10540655-0031018",
        "6C 124133+40580",
        "@l%bam$",
        "Abstraction Latin Patricia Phelps de Cisneros Collection",
        "Ameriaca",
        "America",
        "American rapper",
        "Apaizac beto expeditin",
        "Big Blue",
        "Catholic archbishop",
        "China",
        "Chuck Palahichikkk",
        "Church of St Adrew",
        "Colin Rand Kapernikuss",
        "Communism[citation needed]]",
        "Expedition 56",
        "Floridaaa",
        "Geometric Abstraction: Latin American Art from the Patricia Phelps de Cisneros Collection",
        "H. Tjkeda",
        "Hadeki Tjkeda",
        "Hedeki Tjkeda",
        "Hideaki Takeda",
        "Hidzki",
        "Huyn Naông Cn",
        "La gran bretaña",
        "M. Nykänen",
        "Mark Mcgr",
        "Matthew Macanhey",
        "Matthew Macaunhay",
        "Mitochondrial Uncoupling Proteins",
        "New York",
        "Oregon, OR",
        "PIR protein, pseudogene",
        "Paulys Realenzyklopädie der klassischen Altertumswissenschaft",
        "Phoenix",
        "Phucc Nguyan",
        "Picubah Street",
        "Préfecture de Kanagawa",
        "Q18845165",
        "R. Millar",
        "RE:Aulon 17",
        "Rubidium-7",
        "SDS J100759.52+102152.8",
        "SDSS J001733.60+0040306",
        "Sarah Mclaugling",
        "Sarah Mclauphlan",
        "Sarrah Mcgloclyn",
        "Straßenbahn Haltestelle Wendenschloß",
        "T Kolotilshchikova",
        "T. Kolotilshchikova",
        "Tatyana Kolotilshchikova",
        "Tb10.NT.103",
        "Tokio",
        "Tokyo",
        "Tokyo Olempic",
        "US",
        "Univerity of Belgrade",
        "V* FH Mon",
        "Wikimedia topic category",
        "WâpYên",
        "Zachary Knight Galifianacisss",
        "[RMH2016] 011.243560+41.94550",
        "[rippe vacin",
        "a2erica",
        "aideakiii akea",
        'assassination of"John F. Kennedy',
        "bismuth-393",
        "borough council",
        "chapel",
        "commercial art gallery",
        "corona japan",
        "covid japan",
        "dasdasj",
        "ensemble",
        "entialarials",
        "enzyme family",
        "famsie",
        "hidEAki tAKeda",
        "hideaki takeda",
        "ministry of the State Council",
        "music term",
        "neodymiwm-155",
        "neod{mium-133",
        "partly free country",
        "rUBIdium-7",
        "rUbidIUm-2",
        "rest area",
        "rubIDIum-7",
        "rubidium-7",
        "rural municipality of Estonia",
        "ruthenium-:8",
        "scientific article",
        "semtab",
        "sports festival",
        "titaniu-75",
        "titanium-4",
        "titanium-75",
        "tokyo",
        "wildlife management area",
        "{irconium-83",
        "Град Скопјее",
        "অ্যাটলেটিকো ডি কলকাতা",
        "日本情報学研究所",
        "武田英明",
        "제주 유나이티드 FC",
    ]

    start_1 = time()
    c_ok = 0
    for query in queries:
        iw.print_status("\nQuery: %s" % query)
        start = time()
        responds = search(query, lang=lang, mode=mode, expensive=expensive, limit=limit)
        iw.print_status(
            f"About {len(responds)} results ({(time() - start):.5f} seconds)"
        )
        responds = wiki_items.get_items_info(responds[:3])
        if responds:
            c_ok += 1
        for i, respond in enumerate(responds):
            r_score = respond["score"]
            r_label = respond["label"]
            r_wd = respond["wikidata"]
            # r_des = respond["description"]
            iw.print_status(
                f"{i + 1:2d}. " f"{r_score * 100:5.2f}| " f"{r_wd} | [{r_label}]"
            )
    iw.print_status(f"{c_ok}/{len(queries)} - Run time {time() - start_1:.10f} seconds")


if __name__ == "__main__":
    init()
    # Aggregation Search
    # run_entity_search(lang="en", mode="b")
    run_entity_search(lang="all", mode="b")
