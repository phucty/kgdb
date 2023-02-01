import csv
import re
from datetime import datetime
from enum import Enum

# Configuration
from typing import List, Set

import psutil

ENCODING = "utf-8"

################################################################################
# Dumps Version
################################################################################
VER_WD_SQL = "20220601"
VER_WD_TRUTHY = "20220521"
VER_WD_JSON = "20220606"
VER_WP = "20220601"


# Directories
DIR_ROOT = "/Users/phucnguyen/git/kgdb"
DIR_DUMP = "/Users/phucnguyen/git/dumps"
DIR_DATABASES = "/Users/phucnguyen/git/databases"
DIR_CONFIG = f"{DIR_ROOT}/kgdb/config"

# Log
FORMAT_DATE = "%Y_%m_%d_%H_%M"
DIR_LOG = f"{DIR_ROOT}/log/{datetime.now().strftime(FORMAT_DATE)}.txt"

################################################################################
# Dumps Directories
################################################################################
DIR_DUMPS_WD = f"{DIR_DUMP}/wikidata"
DIR_DUMPS_WP = f"{DIR_DUMP}/wikipedia"
DIR_DUMPS_DP = f"{DIR_DUMP}/dbpedia/20220705"

DIR_DUMP_WD_JSON = f"{DIR_DUMPS_WD}/wikidata-{VER_WD_JSON}-all.json.bz2"
DIR_DUMP_WD_TRUTHY = f"{DIR_DUMPS_WD}/latest-truthy.nt.bz2"
DIR_DUMP_WD_PAGE = f"{DIR_DUMPS_WD}/wikidatawiki-{VER_WD_SQL}-page.sql.gz"
DIR_DUMP_WD_REDIRECT = f"{DIR_DUMPS_WD}/wikidatawiki-{VER_WD_SQL}-redirect.sql.gz"

DIR_DUMP_WP_EN = f"{DIR_DUMPS_WP}/enwiki-{VER_WP}-pages-articles.xml.bz2"
# Wikipedia SQL dump for ID mapping - Wikipedia - Wikidata
DIR_DUMP_WP_PAGE = f"{DIR_DUMPS_WP}/enwiki-{VER_WP}-page.sql.gz"
DIR_DUMP_WP_PROPS = f"{DIR_DUMPS_WP}/enwiki-{VER_WP}-page_props.sql.gz"
DIR_DUMP_WP_REDIRECT = f"{DIR_DUMPS_WP}/enwiki-{VER_WP}-redirect.sql.gz"

# DBpedia Data bus dump
DIR_DUMP_DP_WP = f"{DIR_DUMPS_DP}/wikipedia-links_lang=en.ttl.bz2"

DIR_DUMP_DP_WD = f"{DIR_DUMPS_DP}/ontology--DEV_type=parsed_sorted.nt"
DIR_DUMP_DP_REDIRECT = f"{DIR_DUMPS_DP}/redirects_lang=en.ttl.bz2"

DIR_DUMP_DP_LABELS = f"{DIR_DUMPS_DP}/labels_lang=en.ttl.bz2"
DIR_DUMP_DP_DESC = f"{DIR_DUMPS_DP}/short-abstracts_lang=en.ttl.bz2"
DIR_DUMP_DP_TYPES_SPECIFIC = f"{DIR_DUMPS_DP}/instance-types_lang=en_specific.ttl.bz2"
DIR_DUMP_DP_TYPES_TRANSITIVE = (
    f"{DIR_DUMPS_DP}/instance-types_lang=en_transitive.ttl.bz2"
)
DIR_DUMP_DP_INFOBOX = f"{DIR_DUMPS_DP}/infobox-properties_lang=en.ttl.bz2"
DIR_DUMP_DP_OBJECTS = f"{DIR_DUMPS_DP}/mappingbased-objects_lang=en.ttl.bz2"
DIR_DUMP_DP_LITERALS = f"{DIR_DUMPS_DP}/mappingbased-literals_lang=en.ttl.bz2"
DIR_DUMP_DP_DISAMBIGUATION = f"{DIR_DUMPS_DP}/disambiguations_lang=en.ttl.bz2"


DIR_WIKIDB = f"{DIR_DATABASES}/wikidata"
DIR_DPDB = f"{DIR_DATABASES}/dbpedia"
DIR_WPDB = f"{DIR_DATABASES}/wikipedia"
DIR_DB_LABELS = f"{DIR_DATABASES}/labels"
DIR_WIKI_PAGERANK_STATS = f"{DIR_DATABASES}/wiki_graph_pagerank_stats.pkl"
DIR_WIKI_GRAPH_PAGERANK = f"{DIR_DATABASES}/wiki_graph_pagerank.pkl"


class ATTR_OPTS:
    AND = "AND"
    OR = "OR"
    NOT = "NOT"


# Wikidata
WD = "http://www.wikidata.org/entity/"
WDT = "http://www.wikidata.org/prop/direct/"
WDT3 = "http://www.wikidata.org/prop/direct-normalized/"
WDT2 = "http://www.wikidata.org/prop/statement/"
WD_PROP_LABEL = "http://schema.org/name"
WD_PROP_DES = "http://schema.org/description"
WD_PROP_ALIAS = "http://www.w3.org/2004/02/skos/core#altLabel"

# Wikipedia
WIKI_EN = "http://en.wikipedia.org/wiki/"

# DBpedia
DBR = "http://dbpedia.org/resource/"
DBO = "http://dbpedia.org/ontology/"
DBP = "http://dbpedia.org/property/"

# Others
FOAF = "http://xmlns.com/foaf/0.1/"
PURL = "http://purl.org/dc/elements/1.1/"
SKOS = "http://www.w3.org/2004/02/skos/core#"

PREFIX_LIST = {WD, WDT, WDT2, DBR, DBO, DBP, WIKI_EN, FOAF, PURL}


def read_tsv_file_first_col(file_name) -> List[str]:
    with open(file_name, encoding=ENCODING) as f:
        first_col: List[str] = [l[0].rstrip() for l in csv.reader(f, delimiter="\t")]
    return first_col


WIKIDATA_IDENTIFIERS: Set[str] = set(
    read_tsv_file_first_col(f"{DIR_CONFIG}/WD_IDENTIFIERS.tsv")
)
# WP_IGNORED_NS = read_tsv_file_first_col(f"{DIR_CONFIG}/WP_IGNORED_NS.tsv")
# 105 languages as mGENRE De Cao et al.
# LANGS_105 = read_tsv_file_first_col(f"{DIR_CONFIG}/LANGS_105.tsv")
# 322 languages of Wikipedia
LANGS_322: List[str] = read_tsv_file_first_col(f"{DIR_CONFIG}/LANGS_322.tsv")
# LANGS_SELECTED = read_tsv_file_first_col(f"{DIR_CONFIG}/LANGS_SELECTED.tsv")
LANGS: List[str] = LANGS_322

WP_NAMESPACE_RE = re.compile(r"^{(.*?)}")
WP_DISAMBIGUATE_REGEXP = re.compile(
    r"{{\s*(disambiguation|disambig|disamb|dab|geodis)\s*(\||})", re.IGNORECASE
)

HTML_HEADERS: List[str] = read_tsv_file_first_col(f"{DIR_CONFIG}/TAGS_HTML_HEADERS.tsv")
SPACY_NER_TAGS: List[str] = read_tsv_file_first_col(f"{DIR_CONFIG}/NER_TAGS_SPACY.tsv")
LANGS_SPACY: List[str] = read_tsv_file_first_col(f"{DIR_CONFIG}/LANGS_SPACY.tsv")
WP_IGNORED_NS = (
    "wikipedia:",
    "file:",
    "portal:",
    "template:",
    "mediawiki:",
    "user:",
    "help:",
    "book:",
    "draft:",
    "module:",
    "timedtext:",
)

SIZE_1MB = 1_048_576
SIZE_1GB = 1_073_741_824

LMDB_MAX_KEY = 511
LMDB_MAP_SIZE = 10_737_418_240  # 10GB
# Using Ram as buffer
LMDB_BUFF_BYTES_SIZE = psutil.virtual_memory().total // 10
if LMDB_BUFF_BYTES_SIZE > SIZE_1GB:
    LMDB_BUFF_BYTES_SIZE = SIZE_1GB


BUFF_LIMIT = SIZE_1GB

WD_ENTITY_NAME_PROPS: List[str] = [
    "P528",  # catalog code
    "P3083",  # SIMBAD ID
    "P3382",  # GeneDB ID
    "P742",  # pseudonym
    "P1845",  # anti-virus alias
    "P8338",  # applies to name of value
    "P5168",  # applies to name of item
    "P1449",  # nickname
    "P4970",  # alternate names
    "P2561",  # name
    "P1448",  # official name
    "P1813",  # short name
    "P1843",  # taxon common name
    "P1476",  # title
    "P735",  # given name
    "P1533",  # family name identical to this given name
    "P5168",  # applies to name of item
    "P1950",  # second family name in Spanish name
    "P8253",  # OSM Name Suggestion Index identifier
    "P960",  # Tropicos scientific name ID
    "P9382",  # Unicode character name
    "P1843",  # taxon common name
    "P4633",  # name of the character role
]
WEIGHT_WD = 3
WEIGHT_TYPES = 1
WEIGHT_W_OTHERS = 1
MAX_EDIT_DISTANCE = 10
WEIGHT_PAGERANK = 3e7
LIMIT_GEN_CAN = 50
LIMIT_SEARCH = 50
LIMIT_SEARCH_ES = 1000
LIMIT_CEA_TAR = 1000
LIMIT_TABLES = 100

LMDB_MAX_KEY = 511
LMDB_MAP_SIZE = 10_737_418_240  # 10GB
LMDB_BUFF_LIMIT = SIZE_1GB

WEIGHT_PR_STD = 4.0571367263503044e-08  # 4.046594838893245e-08
WEIGHT_PR_MEAN = 9.336232695716839e-09  # 1.0656537784491441e-08
WEIGHT_PR_MAX = 0.0001920781011278011  # 0.00011678802493033757
WEIGHT_PR_MIN = 4.445134719226468e-09  # 4.871408368733647e-09

WEIGHT_PR_DIV = WEIGHT_PR_MAX - WEIGHT_PR_MIN
WEIGHT_PR_MEAN_RATIO = (WEIGHT_PR_MEAN - WEIGHT_PR_MIN) / WEIGHT_PR_DIV


# Elastic Search parameters
ES_INDEX_NAME_EN = "mtab_en"
ES_INDEX_NAME_ALL = "mtab_all"
ES_MAPPING = {
    "settings": {
        "number_of_shards": 2,
        "number_of_replicas": 0,
        "codec": "best_compression",
    },
    "mappings": {"properties": {"label": {"type": "text"}}},
}

# Enum
class EnumPr(Enum):
    SOFTMAX = 1
    AVG = 2


class EnumRank(Enum):
    RANK = 1
    SCORE = 2
    EQUAL = 3


class DBUpdateType:
    SET = 0
    COUNTER = 1
