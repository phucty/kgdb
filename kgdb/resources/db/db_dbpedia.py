import datetime
import gc
import json
import numbers
import re
import urllib
from collections import defaultdict
from enum import Enum
from pickle import encode_long
from typing import Any, List, Optional, Union

import rdflib
from freaddb.db_lmdb import DBSpec, ToBytes
from rdflib import BNode, Literal, URIRef
from rdflib.namespace import Namespace, NamespaceManager
from rdflib.util import from_n3
from tqdm import tqdm

from kgdb.config import config as cf
from kgdb.resources.db.db_core import DBCore
from kgdb.resources.db.utils import is_wikidata_item
from kgdb.utils import io_worker as iw


def clean_text_brackets(text) -> str:
    if "[[" in text and "]]" in text and "#" in text:

        tmp_text = re.sub("\[\[(.*)\]\]", "", text)
        if tmp_text == text:
            text = ""
        else:
            text = tmp_text.strip()
    if "[[" in text and "#" in text and "]]" not in text:
        text = re.sub("\[\[(.*)", "", text).strip()
    if "]]" in text and "[[" not in text:
        text = re.sub("(.*)\]\]", "", text).strip()

    if "see" in text.lower():
        text = ""

    if "(" in text and ")" not in text:
        text = text.replace("(", "").strip()
    if ")" in text and "(" not in text:
        text = text.replace(")", "").strip()
    return text


def remove_prefix(ns):
    for prefix in cf.PREFIX_LIST:
        ns = ns.replace(prefix, "")
    return ns


def norm_namespace(ns, is_remove_prefix: bool = True):
    if ns[0] == "<" and ns[-1] == ">":
        ns = ns[1:-1]
    if is_remove_prefix:
        ns = remove_prefix(ns)
    return ns


def parse_triple_line(line, remove_prefix: bool = True):
    if isinstance(line, (bytes, bytearray)):
        line = line.decode(cf.ENCODING)
    triple = line.split(" ", 2)
    # if len(triple) < 2 or len(triple) > 5 \
    #         or not (len(triple) == 4 and triple[-1] == "."):
    #     return None

    tail = triple[2].replace("\n", "")
    tail = tail.strip()
    if tail[-1] == ".":
        tail = tail[:-1]
    tail = tail.strip()

    head = norm_namespace(triple[0], remove_prefix)
    prop = norm_namespace(triple[1], remove_prefix)
    tail = norm_namespace(tail, remove_prefix)
    return head, prop, tail


def norm_wikipedia_title(title, unquote=False):
    if not title or not len(title):
        return title
    if len(title) > 1:
        title = (title[0].upper() + title[1:]).replace("_", " ")
    else:
        title = title[0].upper()
    if unquote:
        title = urllib.parse.unquote(title)  # type: ignore
    return title


def from_n3_fix(obj):
    try:
        obj = from_n3_fix_unicodeescape(obj)
    except KeyError:
        obj = str(obj)
    except Exception as message:
        iw.print_status(message)  # type: ignore
    return obj


def from_n3_fix_unicodeescape(s: str, default=None, backend=None, nsm=None):
    """
    Try not use this in the orignial code of rdflib
    value.encode("raw-unicode-escape").decode("unicode-escape")

    :param s:
    :type s:
    :param default:
    :type default:
    :param backend:
    :type backend:
    :param nsm:
    :type nsm:
    :return:
    :rtype:
    """
    if not s:
        return default
    if s.startswith("<"):
        # Hack: this should correctly handle strings with either native unicode
        # characters, or \u1234 unicode escapes.
        return URIRef(s[1:-1].encode("raw-unicode-escape").decode("unicode-escape"))
    elif s.startswith('"'):
        if s.startswith('"""'):
            quotes = '"""'
        else:
            quotes = '"'
        value, rest = s.rsplit(quotes, 1)
        value = value[len(quotes) :]  # strip leading quotes
        datatype = None
        language = None

        # as a given datatype overrules lang-tag check for it first
        dtoffset = rest.rfind("^^")
        if dtoffset >= 0:
            # found a data type
            # data type has to come after lang-tag so ignore everything before
            # see: http://www.w3.org/TR/2011/WD-turtle-20110809/
            # #prod-turtle2-RDFLiteral
            datatype = from_n3(rest[dtoffset + 2 :], default, backend, nsm)
        else:
            if rest.startswith("@"):
                language = rest[1:]  # strip leading at sign

        value = value.replace(r"\"", '"')
        # unicode-escape interprets \xhh as an escape sequence,
        # but n3 does not define it as such.
        value = value.replace(r"\x", r"\\x")
        # Hack: this should correctly handle strings with either native unicode
        # characters, or \u1234 unicode escapes.
        try:
            value = value.encode("raw-unicode-escape").decode("unicode-escape")
        except UnicodeDecodeError:
            value = str(value)

        return Literal(value, language, datatype)  # type: ignore
    elif s == "true" or s == "false":
        return Literal(s == "true")
    elif s.isdigit():
        return Literal(int(s))
    elif s.startswith("{"):
        identifier = from_n3(s[1:-1])
        return rdflib.graph.QuotedGraph(backend, identifier)  # type: ignore
    elif s.startswith("["):
        identifier = from_n3(s[1:-1])
        return rdflib.graph.Graph(backend, identifier)  # type: ignore
    elif s.startswith("_:"):
        return BNode(s[2:])
    elif ":" in s:
        if nsm is None:
            # instantiate default NamespaceManager and rely on its defaults
            nsm = NamespaceManager(rdflib.graph.Graph())  # type: ignore
        prefix, last_part = s.split(":", 1)
        ns = dict(nsm.namespaces())[prefix]
        return Namespace(ns)[last_part]
    else:
        return BNode(s)


class COLUMN(Enum):
    ID_LID = "ID_LID"
    LID_ID = "LID_ID"

    REDIRECT = "REDIRECT"
    REDIRECT_OF = "REDIRECT_OF"

    WIKIDATA = "WIKIDATA"
    WIKIPEDIA = "WIKIPEDIA"

    LABEL = "LABEL"
    DESC = "DESC"
    ALIASES_EN = "ALIASES_EN"
    ALIASES_ALL = "ALIASES_ALL"
    TYPES_SPECIFIC = "TYPES_SPECIFIC"
    TYPES_TRANSITIVE = "TYPES_TRANSITIVE"
    CLAIMS_ENT = "CLAIMS_ENT"

    CLAIMS_STR = "CLAIMS_STR"
    CLAIMS_TIME = "CLAIMS_TIME"
    CLAIMS_QUANTITY = "CLAIMS_QUANTITY"


DBDP_SCHEMA = {
    COLUMN.ID_LID: DBSpec(COLUMN.ID_LID.value),
    COLUMN.LID_ID: DBSpec(COLUMN.LID_ID.value, integerkey=True),
    COLUMN.REDIRECT: DBSpec(COLUMN.REDIRECT.value, integerkey=True),
    COLUMN.REDIRECT_OF: DBSpec(
        COLUMN.REDIRECT_OF.value, integerkey=True, bytes_value=ToBytes.INT_NUMPY
    ),
    COLUMN.WIKIDATA: DBSpec(COLUMN.WIKIDATA.value, integerkey=True),
    COLUMN.WIKIPEDIA: DBSpec(COLUMN.WIKIPEDIA.value, integerkey=True),
    COLUMN.LABEL: DBSpec(COLUMN.LABEL.value, integerkey=True),
    COLUMN.DESC: DBSpec(COLUMN.DESC.value, integerkey=True),
    COLUMN.ALIASES_EN: DBSpec(
        COLUMN.ALIASES_EN.value, integerkey=True, bytes_value=ToBytes.INT_NUMPY
    ),
    COLUMN.ALIASES_ALL: DBSpec(
        COLUMN.ALIASES_ALL.value, integerkey=True, bytes_value=ToBytes.INT_NUMPY
    ),
    COLUMN.TYPES_SPECIFIC: DBSpec(COLUMN.TYPES_SPECIFIC.value, integerkey=True),
    COLUMN.TYPES_TRANSITIVE: DBSpec(
        COLUMN.TYPES_TRANSITIVE.value, integerkey=True, bytes_value=ToBytes.INT_NUMPY
    ),
    COLUMN.CLAIMS_ENT: DBSpec(
        COLUMN.CLAIMS_ENT.value, combinekey=True, bytes_value=ToBytes.INT_NUMPY
    ),
    COLUMN.CLAIMS_STR: DBSpec(COLUMN.CLAIMS_STR.value, combinekey=True),
    COLUMN.CLAIMS_TIME: DBSpec(COLUMN.CLAIMS_TIME.value, combinekey=True),
    COLUMN.CLAIMS_QUANTITY: DBSpec(COLUMN.CLAIMS_QUANTITY.value, combinekey=True),
}


class DBDBpedia(DBCore):
    def __init__(
        self,
        db_file: str = cf.DIR_DPDB,
        db_schema: Optional[List[DBSpec]] = DBDP_SCHEMA.values(),  # type: ignore
        readonly: bool = True,
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

    def build_redirects(self):
        self._parse_ttl_1_1(
            column_name=COLUMN.REDIRECT.value,
            file=cf.DIR_DUMP_DP_REDIRECT,
            message="REDIRECT",
            tail_is_item=True,
            column_name_inv=COLUMN.REDIRECT_OF.value,
            encode_key=True,
            encode_value=True,
            encode_key_inv=True,
            encode_value_inv=True,
        )
        # Parse REDIRECT - 10,100,000 - buff: 90.89 %: : 10100000it [12:17, 13697.21it/s]

    def build_mapping(self):
        self._build_mapping_wikipedia()
        self._build_mapping_wikidata()

    def build_information(self):
        self._parse_ttl_1_1(
            column_name=COLUMN.LABEL.value,
            file=cf.DIR_DUMP_DP_LABELS,
            message="LABEL",
            encode_key=True,
            encode_value=True,
        )
        # Parse REDIRECT - 10,100,000 - buff: 90.89 %: : 10100000it [12:17, 13697.21it/s]

        self._parse_ttl_1_1(
            column_name=COLUMN.DESC.value,
            file=cf.DIR_DUMP_DP_DESC,
            message="DESC",
            encode_key=True,
        )
        # Parse DESC - 5,509,999 - buff: 79.83 %: : 5510000it [08:11, 11216.64it/s]

        self._parse_ttl_1_1(
            column_name=COLUMN.TYPES_SPECIFIC.value,
            file=cf.DIR_DUMP_DP_TYPES_SPECIFIC,
            message="Specific Types",
            encode_key=True,
            encode_value=True,
        )
        # Parse Specific Types - 7,500,000 - buff: 80.30 %: : 7500000it [05:44, 21761.66it/s]

        self._parse_ttl_1_n(
            column_name=COLUMN.TYPES_TRANSITIVE.value,
            file=cf.DIR_DUMP_DP_TYPES_TRANSITIVE,
            message="Transitive Types",
            encode_key=True,
            encode_value=True,
        )

        self._parse_facts()
        # DBpedia mapping literals: 10000it [00:00, 21555.50it/s]
        # Parse Strings - 16,884 - buff: 0.10 %: : 8440000it [00:00, 17927861.86it/s]
        # Parse Times - 2,263 - buff: 0.11 %: : 1130000it [00:00, 21175687.36it/s]
        # Parse Quantities - 4,425 - buff: 0.13 %: : 2210000it [00:00, 32893350.08it/s]
        # DBpedia mapping objects: 10000it [00:00, 25718.72it/s]
        # Parse Entities - 16,967 - buff: 0.21 %: : 8480000it [00:00, 16363806.05it/s]

        self._parse_aliases_multilingual()
        self._parse_aliases()
        self.compress()

    def _build_mapping_wikipedia(self):
        from kgdb.resources.db.db_wikipedia import DBWikipedia

        dbwp = DBWikipedia(readonly=True)
        for line in tqdm(
            iw.read_line_from_file(cf.DIR_DUMP_DP_WP),
            desc="Mapping DBpedia title -> Wikipedia title",
            mininterval=2,
        ):
            respond = parse_triple_line(line)
            if not respond:
                continue
            wp_title, dp_prop, dp_title = respond
            if dp_prop != "isPrimaryTopicOf":
                continue
            wp_title = norm_wikipedia_title(wp_title, unquote=True)
            dp_title = norm_wikipedia_title(dp_title, unquote=True)
            if wp_title and dp_title:
                wp_title = dbwp.get_redirect(wp_title)
                self.add_buff_with_lid(
                    COLUMN.WIKIPEDIA.value, dp_title, wp_title, encode_key=True
                )
        self.save_buff_lid()

    def _build_mapping_wikidata(self):
        from kgdb.resources.db.db_wikidata import DBWikidata

        dbwd = DBWikidata(readonly=True)
        for line in tqdm(
            iw.read_line_from_file(cf.DIR_DUMP_DP_WD),
            desc="Mapping DBpedia classes, properties -> Wikidata ID",
            mininterval=2,
        ):
            respond = parse_triple_line(line)
            if not respond or "wikidata" not in line:
                continue
            dp_title, _, wd_id = respond
            dp_title = norm_wikipedia_title(dp_title, unquote=True)
            wd_id = remove_prefix(wd_id)

            if wd_id and dp_title and is_wikidata_item(wd_id):

                wd_id_redirect = dbwd.get_redirect(wd_id)
                self.add_buff_with_lid(
                    COLUMN.WIKIDATA.value, dp_title, wd_id_redirect, encode_key=True
                )
        self.save_buff_lid()

    def _parse_ttl_1_1(
        self,
        column_name: str,
        file: str,
        message: str = "",
        tail_is_item: bool = False,
        column_name_inv: Optional[str] = None,
        encode_key: bool = False,
        encode_value: bool = False,
        encode_key_inv: bool = False,
        encode_value_inv: bool = False,
        step=10000,
    ):
        """
        Parse DBpedia dump file as subject - predicate - object: 1 - 1 relation
        """
        buff_objs_inv = defaultdict(set)
        p_bar = tqdm(desc=self.update_desc(column_name, message), mininterval=3)

        for i, line in enumerate(iw.read_line_from_file(file)):
            if i and i % step == 0:
                p_bar.update(step)
                p_bar.set_description(self.update_desc(column_name, message))
                # break

            if "dbpedia" not in str(line):
                continue

            respond = parse_triple_line(line)
            if not respond:
                continue

            dp_title, _, dp_value = respond
            dp_title = norm_wikipedia_title(dp_title, unquote=True)
            if dp_title:
                if tail_is_item:
                    dp_value = norm_wikipedia_title(dp_value, unquote=True)
                else:
                    dp_value = str(from_n3_fix(dp_value))
                if dp_value:
                    if column_name_inv:
                        buff_objs_inv[dp_value].add(dp_title)
                    self.add_buff_with_lid(
                        column_name,
                        dp_title,
                        dp_value,
                        encode_key=encode_key,
                        encode_value=encode_value,
                    )
        p_bar.close()

        if column_name_inv:
            for k, v in buff_objs_inv.items():
                self.add_buff_with_lid(
                    column_name_inv,
                    k,
                    v,
                    encode_key=encode_key_inv,
                    encode_value=encode_value_inv,
                )
        self.save_buff_lid()

    def _parse_ttl_1_n(
        self,
        column_name: str,
        file: str,
        message: str = "",
        encode_key: bool = False,
        encode_value: bool = False,
        step=10000,
    ):
        p_bar = tqdm(self.update_desc(column_name, message))
        db_n = defaultdict(set)
        for i, line in enumerate(iw.read_line_from_file(file)):
            if i and i % step == 0:
                p_bar.update(step)

            if "dbpedia" not in str(line):
                continue

            respond = parse_triple_line(line, remove_prefix=False)
            if not respond:
                continue

            dp_title, _, dp_obj = respond
            if dp_title and dp_obj and cf.DBR in dp_title:
                dp_title = dp_title.replace(cf.DBR, "")
                dp_title = norm_wikipedia_title(dp_title, unquote=True)
                if not dp_title:
                    continue

                #  Only get DBpedia classes
                if cf.DBO in dp_obj or cf.DBR in dp_obj:
                    dp_obj = dp_obj.replace(cf.DBO, "")
                    dp_obj = dp_obj.replace(cf.DBR, "")
                    dp_obj = norm_wikipedia_title(dp_obj, unquote=True)
                    dp_obj = self.get_redirect(dp_obj)
                    if dp_obj:
                        db_n[dp_title].add(dp_obj)

                # elif cf.WD in dp_obj:
                #     dp_obj = dp_obj.replace(cf.WD, "")

        for i, (k, v) in enumerate(db_n.items()):
            if i and i % step == 0:
                p_bar.update(step)
                p_bar.set_description(self.update_desc(column_name, message))

            self.add_buff_with_lid(
                column_name,
                k,
                v,
                encode_key=encode_key,
                encode_value=encode_value,
            )
        p_bar.close()
        self.save_buff_lid()

    def _parse_aliases(self, step=10000):
        buff_obj = defaultdict(set)
        disambiguation_aliases = defaultdict(set)
        for i, line in tqdm(
            enumerate(iw.read_line_from_file(cf.DIR_DUMP_DP_DISAMBIGUATION)),
            desc="DBpedia Disambiguation",
            mininterval=3,
        ):
            # if i and i % step == 0:
            #     break
            if "dbpedia" not in str(line):
                continue

            respond = parse_triple_line(line)
            if not respond:
                continue
            dp_title, _, dp_value = respond
            dp_title = norm_wikipedia_title(dp_title, unquote=True)
            dp_title = dp_title.replace("(disambiguation)", "").strip()

            if dp_title:
                dp_value = str(from_n3_fix(dp_value))
                if dp_value:
                    dp_value = norm_wikipedia_title(dp_value, unquote=True)
                    disambiguation_aliases[dp_value].add(dp_title)

        for i, dp_title in tqdm(
            enumerate(self.get_db_iter(COLUMN.ID_LID.value, get_values=False)),
            mininterval=3,
            desc="Get other names",
        ):
            # if i and i % step == 0:
            #     break
            for prop_name in ["otherName", "alias"]:
                dp_obj = self.get_claims_string_with_property(dp_title, prop_name)
                if dp_obj is None:
                    continue

                for dp_label in dp_obj:  # type: ignore
                    if (
                        "(disambiguation)" in dp_label
                        and "(disambiguation)" not in dp_title
                    ):
                        continue
                    buff_obj[dp_title].add(dp_label)

            redirects_of = self.get_redirect_of(dp_title)
            if redirects_of:
                for dp_label in redirects_of:  # type: ignore
                    if (
                        "(disambiguation)" in dp_label
                        and "(disambiguation)" not in dp_title
                    ):
                        continue
                    buff_obj[dp_title].add(dp_label)

            ambiguous_labels = disambiguation_aliases.get(dp_title)
            if ambiguous_labels:
                for dp_label in ambiguous_labels:
                    if (
                        "(disambiguation)" in dp_label
                        and "(disambiguation)" not in dp_title
                    ):
                        continue
                    buff_obj[dp_title].add(dp_label)
        p_bar = tqdm(self.update_desc(COLUMN.ALIASES_EN.value, "ALIASES EN"))

        for i, (k, v) in enumerate(buff_obj.items()):
            if i and i % step == 0:
                p_bar.update(step)
                p_bar.set_description(
                    self.update_desc(COLUMN.ALIASES_EN.value, "ALIASES EN")
                )

            self.add_buff_with_lid(
                COLUMN.ALIASES_EN.value,
                k,
                v,
                encode_key=True,
                encode_value=True,
            )
        self.save_buff_lid()

    def _parse_aliases_multilingual(self, step=10000):
        buff_obj = defaultdict(set)
        langs = {
            "ar",
            "ca",
            "cs",
            "de",
            "el",
            "eo",
            "es",
            "eu",
            "fr",
            "ga",
            "id",
            "it",
            "ja",
            "ko",
            "nl",
            "pl",
            "pt",
            "ru",
            "sv",
            "uk",
            "zh",
        }
        for lang in langs:
            dump_file = f"{cf.DIR_DUMPS_DP}/labels_lang={lang}_uris=en.ttl.bz2"
            for line in tqdm(
                iw.read_line_from_file(dump_file), desc=f"DBpedia labels: {lang}"
            ):
                if "dbpedia" not in str(line):
                    continue

                respond = parse_triple_line(line)
                if not respond:
                    continue
                dp_title, _, dp_value = respond
                dp_title = norm_wikipedia_title(dp_title, unquote=True)
                if dp_title:
                    dp_value = str(from_n3_fix(dp_value))
                    if dp_value:
                        buff_obj[dp_title].add(dp_value)

        p_bar = tqdm(self.update_desc(COLUMN.ALIASES_ALL.value, "Aliases all"))
        for i, (k, v) in enumerate(buff_obj.items()):
            if i and i % step == 0:
                p_bar.update(step)
                p_bar.set_description(
                    self.update_desc(COLUMN.ALIASES_ALL.value, "Aliases all")
                )

            self.add_buff_with_lid(
                COLUMN.ALIASES_ALL.value,
                k,
                v,
                encode_key=True,
                encode_value=True,
            )
        self.save_buff_lid()

    def _parse_facts(self, step=10000):
        """
        Parse DBpedia facts from Infobox and mapping of objects and literals
        :return:
        :rtype:
        """
        buff_entities = defaultdict(set)
        buff_strings = defaultdict(set)
        buff_times = defaultdict(set)
        buff_quantities = defaultdict(set)

        def update_literal(dp_title, dp_prop, dp_value):
            dp_value = from_n3_fix(dp_value)
            if dp_value and isinstance(dp_value, str):
                buff_strings[(dp_title, dp_prop)].add(dp_value)
            if dp_value and isinstance(dp_value, Literal):
                if isinstance(dp_value.value, datetime.date) or isinstance(
                    dp_value.value, datetime.datetime
                ):
                    buff_times[(dp_title, dp_prop)].add(str(dp_value.value))
                elif isinstance(dp_value.value, numbers.Number):
                    buff_quantities[(dp_title, dp_prop)].add(dp_value.value)
                else:
                    if not dp_value.value:
                        dp_value = str(dp_value.toPython())
                    else:
                        dp_value = str(dp_value.value)
                    tmp = clean_text_brackets(dp_value)
                    if tmp:
                        buff_strings[(dp_title, dp_prop)].add(tmp)

        for i, line in tqdm(
            enumerate(iw.read_line_from_file(cf.DIR_DUMP_DP_INFOBOX)),
            desc="DBpedia infobox",
            mininterval=3,
        ):
            # if i and i % step == 0:
            # break

            respond = parse_triple_line(line, remove_prefix=False)
            if not respond:
                continue
            dp_title, dp_prop, dp_value = respond
            if dp_title and dp_value and dp_prop and cf.DBR in dp_title:
                dp_title = dp_title.replace(cf.DBR, "")
                dp_title = norm_wikipedia_title(dp_title, unquote=True)
                if not dp_title:
                    continue
                dp_prop = remove_prefix(dp_prop)
                if cf.DBR not in dp_value:
                    continue
                dp_value = dp_value.replace(cf.DBR, "")
                dp_value = norm_wikipedia_title(dp_value, unquote=True)
                dp_value = self.get_redirect(dp_value)
                if dp_title and dp_value:
                    buff_entities[(dp_title, dp_prop)].add(dp_value)
                else:
                    if not dp_value:
                        continue
                    update_literal(dp_title, dp_prop, dp_value)

        for i, line in tqdm(
            enumerate(iw.read_line_from_file(cf.DIR_DUMP_DP_LITERALS)),
            desc="DBpedia mapping literals",
            mininterval=3,
        ):
            # if i and i % step == 0:
            # break
            respond = parse_triple_line(line, remove_prefix=False)
            if not respond:
                continue
            dp_title, dp_prop, dp_value = respond
            if dp_title and cf.DBR in dp_title and dp_value and dp_prop:
                dp_title = dp_title.replace(cf.DBR, "")
                dp_title = norm_wikipedia_title(dp_title, unquote=True)
                if not dp_title:
                    continue
                dp_prop = remove_prefix(dp_prop)
                if not dp_value:
                    continue
                update_literal(dp_title, dp_prop, dp_value)

        def save_data(column_name, buff, message, encode_key=True, encode_value=True):
            p_bar = tqdm(self.update_desc(column_name, message))
            for i, (k, v) in enumerate(buff.items()):
                if i and i % 10 == 0:
                    p_bar.update(step)
                    p_bar.set_description(self.update_desc(column_name, message))
                self.add_buff_with_lid(
                    column_name,
                    k,
                    v,
                    encode_key=encode_key,
                    encode_value=encode_value,
                )
            p_bar.close()

        save_data(
            COLUMN.CLAIMS_STR.value,
            buff_strings,
            message="Strings",
            encode_value=False,
        )
        del buff_strings
        gc.collect()
        save_data(
            COLUMN.CLAIMS_TIME.value,
            buff_times,
            message="Times",
            encode_value=False,
        )
        del buff_times
        gc.collect()

        save_data(
            COLUMN.CLAIMS_QUANTITY.value,
            buff_quantities,
            message="Quantities",
            encode_value=False,
        )
        del buff_quantities
        gc.collect()

        for i, line in tqdm(
            enumerate(iw.read_line_from_file(cf.DIR_DUMP_DP_OBJECTS)),
            desc="DBpedia mapping objects",
            mininterval=3,
        ):
            # if i and i % step == 0:
            # break
            respond = parse_triple_line(line, remove_prefix=False)
            if not respond:
                continue
            dp_title, dp_prop, dp_value = respond
            if (
                dp_title
                and cf.DBR in dp_title
                and dp_prop
                and cf.DBO in dp_prop
                and dp_value
            ):
                dp_title = dp_title.replace(cf.DBR, "")
                dp_title = norm_wikipedia_title(dp_title, unquote=True)
                dp_value = dp_value.replace(cf.DBR, "")
                dp_prop = dp_prop.replace(cf.DBO, "")
                dp_value = norm_wikipedia_title(dp_value, unquote=True)
                dp_value = self.get_redirect(dp_value)
                if dp_title and dp_value:
                    buff_entities[(dp_title, dp_prop)].add(dp_value)

        save_data(COLUMN.CLAIMS_ENT.value, buff_entities, message="Entities")

        self.save_buff_lid()

    def get_item(
        self,
        title: str,
        get_all: bool = False,
        get_label: bool = True,
        get_desc: bool = True,
        get_aliases_en: bool = False,
        get_aliases_all: bool = False,
        get_type_specific: bool = True,
        get_types_transitive: bool = False,
        get_claims_entity: bool = False,
        get_claims_string: bool = False,
        get_claims_time: bool = False,
        get_claims_quantity: bool = False,
    ):
        responds = dict()

        if get_all or get_label:
            responds["label"] = self.get_label(title)
        if get_all or get_desc:
            responds["desc"] = self.get_descriptions(title)
        if get_all or get_aliases_en:
            responds["aliases_en"] = self.get_aliases_en(title)
        if get_all or get_aliases_all:
            responds["aliases_multilingual"] = self.get_aliases_all(title)
        if get_all or get_type_specific:
            responds["types_specific"] = self.get_types_specific(title)
        if get_all or get_types_transitive:
            responds["types_transitive"] = self.get_types_transitive(title)
        if get_all or get_claims_entity:
            responds["claims_entity"] = self.get_claims_entity(title)
        if get_all or get_claims_string:
            responds["claims_string"] = self.get_claims_string(title)
        if get_all or get_claims_time:
            responds["claims_time"] = self.get_claims_time(title)
        if get_all or get_claims_quantity:
            responds["claims_quantity"] = self.get_claims_quantity(title)
        return responds

    def get_wikipedia(self, title: Union[int, str]):
        return self._get_db_item(COLUMN.WIKIPEDIA.value, title, decode_value=False)

    def get_wikidata(self, title: Union[int, str]):
        return self._get_db_item(COLUMN.WIKIDATA.value, title, decode_value=False)

    def get_label(self, title: Union[int, str]):
        return self._get_db_item(COLUMN.LABEL.value, title)

    def get_descriptions(self, title: Union[int, str]):
        return self._get_db_item(COLUMN.DESC.value, title, decode_value=False)

    def get_aliases_en(self, title: Union[int, str]):
        return self._get_db_item(COLUMN.ALIASES_EN.value, title, decode_value=True)

    def get_aliases_all(self, title: Union[int, str]):
        return self._get_db_item(COLUMN.ALIASES_ALL.value, title, decode_value=True)

    def get_types_specific(self, title, decode_value: bool = True):
        return self._get_db_item(
            COLUMN.TYPES_SPECIFIC.value, title, decode_value=decode_value
        )

    def get_types_transitive(self, title, decode_value: bool = True):
        return self._get_db_item(
            COLUMN.TYPES_TRANSITIVE.value, title, decode_value=decode_value
        )

    def get_claims_entity_with_property(self, title, prop, decode_value: bool = True):
        return self._get_db_item(
            COLUMN.CLAIMS_ENT.value, (title, prop), decode_value=decode_value
        )

    def get_claims_entity(self, title, decode_value: bool = True):
        return self._get_db_item_prefix(
            COLUMN.CLAIMS_ENT.value, (title,), decode_value=decode_value
        )

    def get_claims_string_with_property(self, title, prop):
        return self._get_db_item(
            COLUMN.CLAIMS_STR.value, (title, prop), decode_value=False
        )

    def get_claims_string(self, title: Union[int, str]):
        return self._get_db_item_prefix(
            COLUMN.CLAIMS_STR.value, (title,), decode_value=False
        )

    def get_claims_time_with_property(self, title, prop):
        return self._get_db_item(
            COLUMN.CLAIMS_TIME.value, (title, prop), decode_value=False
        )

    def get_claims_time(self, title: Union[int, str]):
        return self._get_db_item_prefix(
            COLUMN.CLAIMS_TIME.value, (title,), decode_value=False
        )

    def get_claims_quantity_with_property(self, title, prop):
        return self._get_db_item(
            COLUMN.CLAIMS_QUANTITY.value, (title, prop), decode_value=False
        )

    def get_claims_quantity(self, title: Union[int, str]):
        return self._get_db_item_prefix(
            COLUMN.CLAIMS_QUANTITY.value, (title,), decode_value=False
        )


if __name__ == "__main__":
    db = DBDBpedia(readonly=True)
    print(json.dumps(db.stats(), indent=2))

# {
#   "directory": "/Users/phucnguyen/git/databases/dbpedia/dbpedia",
#   "size": "8.9GiB",
#   "items": {
#     "ID_LID": 28,325,649,
#     "LID_ID": 28,325,649,
#     "REDIRECT": 10,101,124,
#     "REDIRECT_OF": 3,544,644,
#     "WIKIDATA": 0,
#     "WIKIPEDIA": 0,
#     "LABEL": 16,428,869,
#     "DESC": 5,515,836,
#     "ALIASES_EN": 4,127,557,
#     "ALIASES_ALL": 3,810,737,
#     "TYPES_SPECIFIC": 7,363,011,
#     "TYPES_TRANSITIVE": 6,367,599,
#     "CLAIMS_ENT": 23,915,322,
#     "CLAIMS_STR": 16,676,932,
#     "CLAIMS_TIME": 5,110,444,
#     "CLAIMS_QUANTITY": 4,325,319
#   },
#   "datatype": {
#     "ID_LID": "<class 'str'>: <class 'int'>",
#     "LID_ID": "<class 'int'>: <class 'str'>",
#     "REDIRECT": "<class 'int'>: <class 'int'>",
#     "REDIRECT_OF": "<class 'int'>: <class 'numpy.ndarray'>",
#     "LABEL": "<class 'int'>: <class 'int'>",
#     "DESC": "<class 'int'>: <class 'str'>",
#     "ALIASES_EN": "<class 'int'>: <class 'numpy.ndarray'>",
#     "ALIASES_ALL": "<class 'int'>: <class 'numpy.ndarray'>",
#     "TYPES_SPECIFIC": "<class 'int'>: <class 'int'>",
#     "TYPES_TRANSITIVE": "<class 'int'>: <class 'numpy.ndarray'>",
#     "CLAIMS_ENT": "<class 'tuple'>: <class 'numpy.ndarray'>",
#     "CLAIMS_STR": "<class 'tuple'>: <class 'list'>",
#     "CLAIMS_TIME": "<class 'tuple'>: <class 'list'>",
#     "CLAIMS_QUANTITY": "<class 'tuple'>: <class 'list'>"
#   },
#   "head": {
#     "ID_LID": "!: 40",
#     "LID_ID": "0: !!!!!!!",
#     "REDIRECT": "0: 1",
#     "REDIRECT_OF": "1: [       0   754529  5323341  5323354  7702657  770",
#     "LABEL": "0: 0",
#     "DESC": "1: When We All Fall Asleep, Where Do We Go? (stylized",
#     "ALIASES_EN": "1: [       0   754529  5323341  5323354  7702657  770",
#     "ALIASES_ALL": "0: [0]",
#     "TYPES_SPECIFIC": "1: 1148584",
#     "TYPES_TRANSITIVE": "1: [ 3943623 17255482]",
#     "CLAIMS_ENT": "(16777216, 19688135): [19725902]",
#     "CLAIMS_STR": "(16777216, 19687532): ['Sebastian Ferrulli', 'Sebastian Ferrulli']",
#     "CLAIMS_TIME": "(17039360, 19687602): ['2015-10-19']",
#     "CLAIMS_QUANTITY": "(16842752, 19687675): [86]"
#   }
# }
