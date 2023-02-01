import bz2
import csv
import gzip
import re
import urllib
from collections import defaultdict
from enum import Enum
from typing import List, Optional
from xml.etree.ElementTree import iterparse

import six
import wikitextparser as wtp
from freaddb.db_lmdb import DBSpec, FReadDB
from regex import P
from tqdm import tqdm

from kgdb.config import config as cf
from kgdb.resources.db.db_core import DBCore
from kgdb.resources.db.utils import ToBytes, is_wikidata_item
from kgdb.utils import io_worker as iw


class COLUMN(Enum):
    ID_LID = "ID_LID"
    LID_ID = "LID_ID"

    REDIRECT = "REDIRECT"
    REDIRECT_OF = "REDIRECT_OF"

    WIKIDATA = "WIKIDATA"

    PAGES = "PAGES"


DBWP_SCHEMA = {
    COLUMN.ID_LID: DBSpec(COLUMN.ID_LID.value),
    COLUMN.LID_ID: DBSpec(COLUMN.LID_ID.value, integerkey=True),
    COLUMN.REDIRECT: DBSpec(COLUMN.REDIRECT.value, integerkey=True),
    COLUMN.REDIRECT_OF: DBSpec(
        COLUMN.REDIRECT_OF.value, integerkey=True, bytes_value=ToBytes.INT_NUMPY
    ),
    COLUMN.PAGES: DBSpec(COLUMN.PAGES.value),
    COLUMN.WIKIDATA: DBSpec(COLUMN.WIKIDATA.value, integerkey=True),
}


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


def parse_reader(responds):
    title, lang, wiki_text, redirect = responds
    wiki_page = WPPage(title, lang, wiki_text, redirect)
    return wiki_page


def norm_wikipedia_title(title, unquote=False):
    if not title or not len(title):
        return title
    if len(title) > 1:
        title = (title[0].upper() + title[1:]).replace("_", " ")
    else:
        title = title[0].upper()
    if unquote:
        title = urllib.parse.unquote(title)
    return title


def wiki_plain_text(org_text, next_parse=1):
    try:
        parse_text = wtp.parse(org_text)
        text = parse_text.plain_text(
            replace_tags=False, replace_bolds_and_italics=False
        )
        for t in parse_text.get_tags():
            text = text.replace(t.string, "")
        if "<" in text and next_parse < 3:
            return wiki_plain_text(text, next_parse + 1)
        return text
    except Exception as message:
        print(f"{message}: {org_text}")
        return org_text


class WPDumpReader(object):
    def __init__(self, dump_file, ignored_ns=cf.WP_IGNORED_NS):
        self._dump_file = dump_file
        self._ignored_ns = ignored_ns
        with bz2.BZ2File(self._dump_file) as f:
            self._lang = re.search(
                r'xml:lang="(.*)"', six.text_type(f.readline())
            ).group(1)

    @property
    def dump_file(self):
        return self._dump_file

    @property
    def language(self):
        return self._lang

    def __iter__(self):
        with bz2.BZ2File(self._dump_file) as f:
            for (title, wiki_text, redirect) in self._extract_pages(f):
                lower_title = title.lower()
                if any([lower_title.startswith(ns) for ns in self._ignored_ns]):
                    continue

                yield [title, self._lang, wiki_text, redirect]

    @staticmethod
    def _extract_pages(in_file):
        def _get_namespace(_tag):
            match_obj = cf.WP_NAMESPACE_RE.match(_tag)
            if match_obj:
                ns = match_obj.group(1)
                if not ns.startswith("http://www.mediawiki.org/xml/export-"):
                    raise ValueError("%s not recognized as MediaWiki namespace" % ns)
                return ns
            else:
                return ""

        def _to_unicode(s):
            if isinstance(s, str):
                return s
            return s.decode(cf.ENCODING)

        elems = (elem for (_, elem) in iterparse(in_file, events=(b"end",)))
        elem = next(elems)

        tag = six.text_type(elem.tag)
        namespace = _get_namespace(tag)
        page_tag = "{%s}page" % namespace
        text_path = "./{%s}revision/{%s}text" % (namespace, namespace)
        title_path = "./{%s}title" % namespace
        redirect_path = "./{%s}redirect" % namespace
        for elem in elems:
            if elem.tag == page_tag:
                title = elem.find(title_path).text
                text = elem.find(text_path).text or ""
                redirect = elem.find(redirect_path)
                if redirect is not None:
                    redirect = norm_wikipedia_title(
                        _to_unicode(redirect.attrib["title"])
                    )

                yield _to_unicode(title), _to_unicode(text), redirect
                elem.clear()


class WPPage(object):
    def __init__(self, title, lang, wiki_text, redirect):
        self.title = title
        self.lang = lang
        self.redirect = redirect
        self.wiki_text = wiki_text
        self.wp_obj = None
        self.wp_wd = None
        if not self.redirect:
            try:
                self.wp_obj, self.wp_wd = self._parse_wikipedia_page(wiki_text)
            except Exception as message:
                iw.print_status(message)
                iw.print_status(wiki_text)

    def __repr__(self):
        return self.title

    def __reduce__(self):
        return_obj = (
            self.__class__,
            (self.title, self.lang, self.wiki_text, self.redirect),
        )
        return return_obj

    @staticmethod
    def _parse_wikipedia_page(wiki_text):
        def norm_text(text):
            text = " ".join(text.split())
            return text

        wp_obj = {
            "claims_wd": defaultdict(set),
            "claims_literal": defaultdict(set),
        }
        wp_wd = None
        if not wiki_text or not len(wiki_text):
            return wp_obj, wp_wd

        w_parser = wtp.parse(wiki_text)
        if not w_parser:
            return None

        for w_section in w_parser.sections:
            # Infobox
            for w_template in w_section.templates:
                if "Infobox" in w_template.name:
                    for w_argument in w_template.arguments:
                        w_prop = " ".join(w_argument.name.split())
                        if not len(w_prop):
                            continue
                        if not len(w_argument.wikilinks):
                            w_value = wiki_plain_text(w_argument.value)
                            w_value = norm_text(w_value)
                            if len(w_value):
                                wp_obj["claims_literal"][w_prop].add(w_value)
                        else:
                            for w_link in w_argument.wikilinks:
                                if not w_link.title:
                                    continue
                                w_value = norm_wikipedia_title(w_link.title)
                                if w_value[:5] == "File:":
                                    continue
                                wp_obj["claims_wd"][w_prop].add(w_value)
            # Sections
            prop_value = "Section: Information"
            if w_section.title:
                prop_value = f"Section: {norm_text(w_section.title)}"

            for w_link in w_section.wikilinks:
                if w_link.title:
                    wp_obj["claims_wd"][prop_value].add(
                        norm_wikipedia_title(w_link.title)
                    )

            # Wikidata mapping
            if w_section.title == "External links":
                for i_templates in w_section.templates:
                    if i_templates.name == "Subject bar":
                        for i_arguments in i_templates.arguments:
                            if i_arguments.name == "d" and is_wikidata_item(
                                i_arguments.value
                            ):
                                wp_wd = i_arguments.value

        return wp_obj, wp_wd


class DBWikipedia(DBCore):
    def __init__(
        self,
        db_file: str = cf.DIR_WPDB,
        db_schema: Optional[List[DBSpec]] = DBWP_SCHEMA.values(),
        readonly=True,
        buff_limit: int = cf.BUFF_LIMIT,
        map_size: int = cf.SIZE_1GB,
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

    def get_wikidata(self, item_id: str):
        return self._get_db_item(COLUMN.WIKIDATA.value, item_id, decode_value=False)

    @staticmethod
    def _is_insert(line):
        return line.startswith("INSERT INTO")

    def build_redirects_and_wikidata_mapping(self, step=100000):
        from kgdb.resources.db.db_wikidata import DBWikidata

        dbwd = DBWikidata(readonly=True)
        map_wp_id_title = defaultdict()
        with gzip.open(
            cf.DIR_DUMP_WP_PAGE, "rt", encoding=cf.ENCODING, newline="\n"
        ) as f:
            p_bar = tqdm(desc="Wikipedia IDs", mininterval=2)
            i = 0
            for line in f:
                if not self._is_insert(line):
                    continue
                # if i > step:
                #     break
                for v in parse_sql_values(line):
                    if v[1] == "0":
                        wp_id = v[0]
                        wp_title = norm_wikipedia_title(v[2])
                        map_wp_id_title[wp_id] = wp_title
                        self.get_lid(wp_title, create_new=True)

                        i += 1
                        p_bar.update()
                        # if i > step:
                        #     break
            p_bar.close()

        buff_obj_inv = defaultdict(set)
        with gzip.open(
            cf.DIR_DUMP_WP_REDIRECT, "rt", encoding="utf-8", newline="\n"
        ) as f:
            p_bar = tqdm(desc="Wikipedia redirects", mininterval=2)
            i = 0

            for line in f:
                if not self._is_insert(line):
                    continue
                # if i > 1000:
                #     break
                for v in parse_sql_values(line):
                    wp_source_id = v[0]
                    wp_source_title = map_wp_id_title.get(wp_source_id)
                    if wp_source_title:
                        wp_target_title = norm_wikipedia_title(v[2])
                        self.add_buff_with_lid(
                            COLUMN.REDIRECT.value,
                            wp_source_title,
                            wp_target_title,
                            encode_key=True,
                            encode_value=True,
                        )

                        buff_obj_inv[wp_target_title].add(wp_source_title)
                        p_bar.update()
                        i += 1
                        # if i > 1000:
                        #     break
            p_bar.close()

            for k, v in tqdm(
                buff_obj_inv.items(), desc="Wikipedia redirects of", mininterval=2
            ):
                self.add_buff_with_lid(
                    COLUMN.REDIRECT_OF.value,
                    k,
                    v,
                    encode_key=True,
                    encode_value=True,
                )

        with gzip.open(cf.DIR_DUMP_WP_PROPS, "r") as f:
            p_bar = tqdm(desc="Mapping Wikipedia title -> Wikidata ID")
            for line in f:
                line = line.decode("utf-8", "ignore")
                if not self._is_insert(line):
                    continue
                for v in parse_sql_values(line):
                    wd_id = v[2]
                    if v[1] == "wikibase_item" and is_wikidata_item(wd_id):
                        wp_title = map_wp_id_title.get(v[0])
                        if not wp_title:
                            continue
                        # Mapping from Wikipedia title to Wikidata ID
                        wp_title = self.get_redirect(wp_title)
                        wd_id_redirect = dbwd.get_redirect(wd_id)
                        self.add_buff_with_lid(
                            COLUMN.WIKIDATA.value,
                            wp_title,
                            wd_id_redirect,
                            encode_key=True,
                        )
                        p_bar.update()
            p_bar.close()
        self.save_buff_lid()

    def build_information(self, step= 1000):
        c_ok = 0
        c_redirect = 0
        iter_items = WPDumpReader(cf.DIR_DUMP_WP_EN)

        p_bar = tqdm(desc=self.update_desc(COLUMN.PAGES.value, "Wikipedia"))
        for i, iter_item in enumerate(iter_items):
            responds = parse_reader(iter_item)
            if i and i % step == 0:
                p_bar.set_description(self.update_desc(COLUMN.PAGES.value, "Wikipedia"))
                p_bar.update(step)
            if not responds:
                continue
            if responds.redirect:
                c_redirect += 1
            else:
                c_ok += 1
                self.add_buff_with_lid(
                    COLUMN.PAGES.value, responds.title, responds.wp_obj, encode_key=True
                )
        p_bar.close()

        self.save_buff_lid()
        self.compress()


def check_overlapping():
    db_wp = DBWikipedia()
    luke_vocab = iw.read_json_file(
        "/Users/phucnguyen/git/mtab_server/data/luke_large_500k_entity_vocab.json"
    )

    turl_dir = "/Users/phucnguyen/git/tools/data/turl_entity_vocab.csv"
    turl_vocab = {}
    with open(turl_dir, "r") as f:
        reader = csv.reader(f, delimiter=",")
        for line in tqdm(reader):
            wp_title, _, freebase_id = line
            turl_vocab[wp_title.replace("_", " ")] = freebase_id

    # luke_vocab = {k.replace(" ", "_"): v for k, v in luke_vocab.items()}

    d_mapping = len(set(luke_vocab.keys()).intersection(turl_vocab.keys()))
    print(f"Direct mapping: {d_mapping:,}")

    re_luke_vocab = {}
    n_luke_vocab = 0
    for k in luke_vocab:
        re_item = db_wp.get_redirect(k)
        if db_wp.is_available(COLUMN.PAGES.value, re_item):
            n_luke_vocab += 1
        # else:
        #     print(k)
        re_luke_vocab[re_item] = k
    print(f"Luke: {len(luke_vocab):,}: {n_luke_vocab:,}")

    re_turl_vocab = {}
    n_turl_vocab = 0
    for k in turl_vocab:
        re_item = db_wp.get_redirect(k)
        if db_wp.is_available(COLUMN.PAGES.value, re_item):
            n_turl_vocab += 1
        # else:
        #     print(k)
        re_turl_vocab[re_item] = k
    print(f"Turl: {len(turl_vocab):,}: {n_turl_vocab:,}")

    d_mapping = len(set(re_luke_vocab.keys()).intersection(re_turl_vocab.keys()))

    print(f"Indirect mapping: {d_mapping:,}")
    debug = 1


if __name__ == "__main__":
    check_overlapping()
