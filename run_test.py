import json
import os
from datetime import timedelta
from time import time

from tqdm import tqdm

from kgdb.config import config as cf
from kgdb.resources.db import db_dbpedia
from kgdb.resources.db.db_dbpedia import DBDBpedia
from kgdb.resources.db.db_deletes import DBDeletes, build_db_deletes
from kgdb.resources.db.db_entity_labels import DB_E_LABEL_COLUMN_NAME, DBELabel
from kgdb.resources.db.db_wikidata import DBWikidata, boolean_search
from kgdb.resources.db.db_wikipedia import DBWikipedia
from kgdb.resources.db_elasticsearch import ESearch
from kgdb.utils import io_worker as iw
from kgdb.utils.benchmark import profile


def step_1_download_dumps(
    ver_wd_json: str = cf.VER_WD_JSON,
    ver_wd_sql: str = cf.VER_WD_SQL,
    dir_wd: str = cf.DIR_DUMPS_WD,
    ver_wp: str = cf.VER_WP,
    dir_wp: str = cf.DIR_DUMPS_WP,
    dir_dp: str = cf.DIR_DUMPS_DP,
    print_status: bool = True,
):

    from kgdb.utils import downloader

    start = time()
    # 1. Download Wikidata dump files
    downloader.download_dump_wikidata_json(
        json_ver=ver_wd_json,
        sql_ver=ver_wd_sql,
        download_dir=dir_wd,
        get_bz2=True,
        print_status=print_status,
    )
    # 2. Download Wikipedia dump file (English)
    downloader.download_dump_wikipedia(
        ver=ver_wp,
        download_dir=dir_wp,
        langs=["en"],
        print_status=print_status,
    )
    # # 3. Download DBpedia dump files
    # downloader.download_dump_dbpedia(download_dir=dir_dp)
    # iw.print_status(f"Download dump files in {timedelta(seconds=time() - start)}")


def step_2_build_resources():
    # dbpedia = DBDBpedia(readonly=False, map_size=cf.SIZE_1GB)
    # dbpedia.compress()
    # wikidata = DBWikidata(
    #     readonly=False, map_size=cf.SIZE_1GB, buff_limit=cf.BUFF_LIMIT * 10
    # )
    # wikipedia = DBWikipedia(readonly=False, map_size=cf.SIZE_1GB)

    # Build_redirects
    # dbpedia.build_redirects()
    # wikidata.build_redirects()
    # wikipedia.build_redirects_and_wikidata_mapping()

    # Build mapping
    # dbpedia.build_mapping()
    # wikidata.build_mapping()

    # Build other information
    # wikipedia.build_information()
    # dbpedia.build_information()

    # --> Wikidata
    # wikidata.build_information()

    # Pagerank
    # labels = DBELabel(read_only=False)
    # labels.build_vocab()
    # labels.build_ranking_list()

    # wikidata.build_db_pagerank()

    # labels.build_ranking_list_with_pagerank()

    # # Build delete database:
    # build_db_deletes("en", prefix_len=10, max_distance=4)
    # # DELETE: 1,642,420,151

    # build_db_deletes("en", prefix_len=14, max_distance=2)
    # # DELETE: 4,532,750,180

    # build_db_deletes("all", prefix_len=10, max_distance=4)
    # # DELETE: 5,500,913,364

    # build_db_deletes("all", prefix_len=14, max_distance=2)
    # DELETE: 7,300,689,810


def run_build_db_wikipedia():
    def run_examples():
        db_readonly = DBWikipedia()
        tmp = db_readonly.get_item("Tokyo")
        tmp = db_readonly.get_item("!!!")

    def build():
        db = DBWikipedia(readonly=False)
        db.build_redirects_and_wikidata_mapping()
        db.build_information()
        """
            REDIRECT: 10,109,249
            REDIRECT_OF: 3,474,643
            PAGES: 8,675,433
            WIKIDATA: 6,688,914
        """
        db.close()
        compact_db()

    def compact_db():
        db = DBWikipedia(readonly=False)

    @profile
    def run_speed(db: DBWikipedia, size=20_000):
        keys = []
        for i, k in enumerate(db.iter_db("PAGES", get_values=False)):
            if i > size:
                break
            keys.append(k)
        db.get_values("PAGES", keys)

    def run_test_speed():
        iw.print_status("Default compression")
        run_speed(db=DBWikipedia())

    build()
    run_examples()
    run_test_speed()


def run_build_db_dbpedia():
    def build():
        db = DBDBpedia(readonly=False)
        db.build_redirects()
        db.build_mapping()
        db.build_information()
        iw.print_status("Done")

        db.close()
        compact_db()

    def compact_db():
        db = DBDBpedia(readonly=False)

    def run_examples():
        db_readonly = DBDBpedia(readonly=True)
        tmp = db_readonly.get_item("Xiasi Dog")
        tmp = db_readonly.get_item("James Adams (character)")
        tmp = db_readonly.get_item("Batman: Arkham City")

    @profile
    def run_speed(db: DBDBpedia, size=200_000):
        keys = []
        for i, k in enumerate(db.keys()):
            if i >= size:
                break
            keys.append(k)
        db.get_values("REDIRECT_OF", keys)

    def run_test_speed():
        iw.print_status("Default compression")
        run_speed(db=DBDBpedia(readonly=True))

    build()
    run_examples()
    run_test_speed()


def run_build_db_wikidata():
    def build():
        db = DBWikidata(readonly=False)
        # # Build_redirects
        # db.build_lid_and_redirects()

        # # Build mapping
        # db.build_mapping()

        # # Build other information
        # db.build_information()

        db.build_db_pagerank()
        """
            QID_LID: 100,968,110
            LID_QID: 100,968,110
            REDIRECT: 3,711,270
            REDIRECT_OF: 2,868,206
            LABEL: 89,863,650
            LABELS: 89,532,336
            DESC: 83,232,278
            ALIASES: 11,211,020
            CLAIMS_ENT: 335,243,240
            CLAIMS_ENT_INV: 78,424,190
            CLAIMS_LIT: 88,818,970
            SITELINKS: 22,667,089
            DBPEDIA: 6,447,454
            WIKIPEDIA: 6,688,898
        """

    def run_examples():
        db_readonly = DBWikidata()
        wikidata_item = "Q31"

        lid = db_readonly.get_lid(wikidata_item)
        qid = db_readonly.get_qid(lid)

        wikipedia = db_readonly.get_wikipedia(lid)
        dbpedia = db_readonly.get_dbpedia(lid)

        redirect = db_readonly.get_redirect(lid)
        redirect_of = db_readonly.get_redirect_of(lid)

        item = db_readonly.get_item(lid)

        label = db_readonly.get_label(lid)

        labels = db_readonly.get_labels(lid)
        label_ja = db_readonly.get_labels(lid, "ja")

        desc = db_readonly.get_desc(lid)
        desc_ja = db_readonly.get_desc(lid, "ja")

        aliases = db_readonly.get_aliases(lid)
        aliases_ja = db_readonly.get_aliases(lid, "ja")

        sitelinks = db_readonly.get_sitelinks(lid)
        sitelinks_en = db_readonly.get_sitelinks(lid, "enwiki")

        wp_ja_title = db_readonly.get_wikipedia_title(lid, "ja")
        wp_ja_link = db_readonly.get_wikipedia_link(lid, "ja")

        types = db_readonly.get_instance_of(lid)
        subclass = db_readonly.get_subclass_of(lid)
        all_types = db_readonly.get_all_types(lid)

        return

    @profile
    def run_examples_boolean_search():
        db = DBWikidata(readonly=True)
        print("1.1. Get all female (Q6581072)")
        boolean_search(db, [[cf.ATTR_OPTS.AND, None, "Q6581072"]], get_qid=False)

        print("1.2. Get all male (Q6581072)")
        boolean_search(db, [[cf.ATTR_OPTS.AND, None, "Q6581097"]], get_qid=False)

        print(
            "2. Get all entities has relation with Graduate University for Advanced Studies (Q2983844)"
        )
        boolean_search(
            db,
            [
                # ??? - Graduate University for Advanced Studies
                [cf.ATTR_OPTS.AND, None, "Q2983844"]
            ],
            get_qid=False,
        )

        print(
            "3. Get all entities who are human, male, educated at Todai, and work at SOKENDAI"
        )
        boolean_search(
            db,
            [
                # instance of - human
                [cf.ATTR_OPTS.AND, "P31", "Q5"],
                # gender - male
                [cf.ATTR_OPTS.AND, "P21", "Q6581097"],
                # educated at - Todai
                [cf.ATTR_OPTS.AND, "P69", "Q7842"],
                # employer - Graduate University for Advanced Studies
                [cf.ATTR_OPTS.AND, "P108", "Q2983844"],
            ],
            get_qid=False,
        )

        print(
            "4. Get all entities that have relation with human, male, Todai, and SOKENDAI"
        )
        boolean_search(
            db,
            [
                # instance of - human
                [cf.ATTR_OPTS.AND, None, "Q5"],
                # gender - male
                [cf.ATTR_OPTS.AND, None, "Q6581097"],
                # educated at - Todai
                [cf.ATTR_OPTS.AND, None, "Q7842"],
                # employer - Graduate University for Advanced Studies
                [cf.ATTR_OPTS.AND, None, "Q2983844"],
            ],
            get_qid=False,
        )

        print(
            "5. Get all entities that have relation with scholarly article or DNA, X-ray diffraction, and Francis Crick and Nature"
        )
        boolean_search(
            db,
            [
                # ? - scholarly article
                [cf.ATTR_OPTS.AND, None, "Q13442814"],
                # ? - DNA
                [cf.ATTR_OPTS.OR, None, "Q7430"],
                # ? - X-ray diffraction
                [cf.ATTR_OPTS.OR, None, "Q12101244"],
                # ? - DNA
                [cf.ATTR_OPTS.OR, None, "Q911331"],
                # Francis Crick
                [cf.ATTR_OPTS.AND, None, "Q123280"],
                # ? - Nature
                [cf.ATTR_OPTS.AND, None, "Q180445"],
            ],
            get_qid=False,
        )

    build()
    # run_examples()
    # run_examples_boolean_search()


def run_build_db_labels():
    def build():
        db = DBELabel(read_only=False)
        db.build_vocab()
        db.build_ranking_list()
        db.build_ranking_list_with_pagerank()
        db.close()
        compact_db()

    def compact_db():
        iw.print_status("Compact DB")
        db = DBELabel(read_only=False)
        db.compact(update_db_size=True)

    def run_examples():
        db = DBELabel(read_only=True)
        tmp = db.get_label_from_lid(1)
        tmp = db.get_lid("Tokyo")
        tmp = db.get_wd_en_ranking_from_label_lid(db.get_lid("tokyo"))
        return

    build()
    # compact_db()
    # run_examples()


def run_build_db_deletes():
    def build():
        build_db_deletes("en", prefix_len=10, max_distance=4)
        # DELETE: 1,642,420,151

        build_db_deletes("en", prefix_len=14, max_distance=2)
        # DELETE: 4,532,750,180

        build_db_deletes("all", prefix_len=10, max_distance=4)
        # DELETE: 5,500,913,364

        build_db_deletes("all", prefix_len=14, max_distance=2)
        # DELETE: 7,300,689,810

        # build_db_deletes("all", prefix_len=12, max_distance=4, from_i=17_645_000)

    build()


def other_runs():
    # run_build_db_dbpedia()
    # run_build_db_wikipedia()
    # run_build_db_wikidata()
    # run_build_db_labels()
    # run_build_db_deletes()

    # build_obj = ESearch()
    # build_obj.build()

    db_readonly = DBWikipedia()
    tmp = db_readonly.get_item("Tokyo")

    tmp = db_readonly.get_item("!!!")


def convert_to_lmdb():
    # db = DBDP()
    # db.to_lmdb(
    #     compress_columns=[
    #         "REDIRECT",
    #         "REDIRECT_OF",
    #         "WIKIDATA",
    #         "WIKIPEDIA",
    #         "LABEL",
    #         "DESC",
    #         "ALIASES_EN",
    #         "ALIASES_ALL",
    #         "TYPES_SPECIFIC",
    #         "TYPES_TRANSITIVE",
    #         "CLAIMS_ENT",
    #         "CLAIMS_LIT",
    #     ],
    # )
    #
    db = DBWikipedia()
    db.to_lmdb(compress_columns=["REDIRECT", "REDIRECT_OF", "PAGES"])

    # db = DBWD()
    # db.to_lmdb(map_size=cf.SIZE_1GB * 200)

    # db = DBELabel()
    # db.to_lmdb(map_size=cf.SIZE_1GB * 60)

    # db = DBDeletes(read_only=False, prefix_len=14, lang="en", max_distance=2,)
    # db.to_lmdb(map_size=cf.SIZE_1GB * 150)

    db = DBDeletes(
        read_only=False,
        prefix_len=14,
        lang="all",
        max_distance=2,
    )
    db.to_lmdb(map_size=cf.SIZE_1GB * 200)

    # db = DBDeletes(read_only=False, prefix_len=10, lang="en", max_distance=4,)
    # db.to_lmdb(map_size=cf.SIZE_1GB * 250)

    # db = DBDeletes(read_only=False, prefix_len=10, lang="all", max_distance=4,)
    # db.to_lmdb(map_size=cf.SIZE_1GB * 500)

    # lmdb_obj = LMDBWorker(
    #     db_file="/Users/phucnguyen/git/mtab_server/data/models/wpdb.lmdb",
    #     readonly=False,
    # )
    # lmdb_obj.compress()


if __name__ == "__main__":
    print(f"PID {os.getpid()}")

    # step_1_download_dumps()
    step_2_build_resources()

    # other_runs()
    # convert_to_lmdb()
