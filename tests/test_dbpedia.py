import json
from os import stat

from kgdb.resources.db.db_dbpedia import DBDBpedia


def test_get_redirect():
    db = DBDBpedia(readonly=True)

    # Input is DBpedia title
    assert db.get_redirect("ToKyo") == "Tokyo"

    # Input is DBpedia lid (mapping from DBpedia title to int)
    toKyo_lid = db.get_lid("ToKyo")
    assert db.get_redirect(toKyo_lid) == "Tokyo"

    tokyo_lid = db.get_lid("Tokyo")
    assert db.get_redirect(toKyo_lid, decode_value=False) == tokyo_lid


def test_get_redirect_of():
    db = DBDBpedia(readonly=True)

    # Input is DBpedia title
    assert len(db.get_redirect_of("Tokyo")) >= 64

    # Input is DBpedia lid (mapping from DBpedia title to int)
    tokyo_lid = db.get_lid("Tokyo")
    assert len(db.get_redirect_of(tokyo_lid)) >= 64


def test_get_label():
    db = DBDBpedia(readonly=True)
    assert db.get_label("Tokyo") == "Tokyo"

    tokyo_lid = db.get_lid("Tokyo")
    assert db.get_label(tokyo_lid) == "Tokyo"


def test_get_descriptions():
    db = DBDBpedia(readonly=True)
    # Check describtion available
    assert db.get_descriptions("Tokyo") is not None

    tokyo_lid = db.get_lid("Tokyo")
    assert db.get_descriptions(tokyo_lid) is not None


def test_get_types_specific():
    db = DBDBpedia(readonly=True)
    assert db.get_types_specific("Tokyo") == "City"

    tokyo_lid = db.get_lid("Tokyo")
    assert db.get_types_specific(tokyo_lid) == "City"

    city_lid = db.get_lid("City")
    assert db.get_types_specific(tokyo_lid, decode_value=False) == city_lid


def test_get_types_transitive():
    db = DBDBpedia(readonly=True)

    assert len(db.get_types_transitive("Tokyo")) > 0

    tokyo_lid = db.get_lid("Tokyo")
    assert len(db.get_types_transitive(tokyo_lid)) > 0

    assert len(db.get_types_transitive(tokyo_lid, decode_value=False)) > 0


def test_get_claims_entity():
    db = DBDBpedia(readonly=True)
    assert len(db.get_claims_entity("Tokyo", decode_value=True)) > 0

    assert len(db.get_claims_entity("Tokyo", decode_value=False)) > 0

    tokyo_lid = db.get_lid("Tokyo")
    assert len(db.get_claims_entity(tokyo_lid, decode_value=True)) > 0


def test_get_claims_entity_with_property():
    db = DBDBpedia(readonly=True)

    result = db.get_claims_entity_with_property(
        "Tokyo", "subdivision", decode_value=True
    )
    assert len(result) > 0

    result = db.get_claims_entity_with_property(
        "Tokyo", "subdivision", decode_value=False
    )
    assert len(result) > 0

    tokyo_lid = db.get_lid("Tokyo")
    result = db.get_claims_entity_with_property(
        tokyo_lid, "subdivision", decode_value=False
    )
    assert len(result) > 0

    subdivision_lid = db.get_lid("subdivision")
    result = db.get_claims_entity_with_property(
        tokyo_lid, subdivision_lid, decode_value=False
    )
    assert len(result) > 0


def test_get_claims_string():
    db = DBDBpedia(readonly=True)
    assert len(db.get_claims_string("Tokyo")) > 0

    tokyo_lid = db.get_lid("Tokyo")
    assert len(db.get_claims_string(tokyo_lid)) > 0


def test_get_claims_string_with_property():
    db = DBDBpedia(readonly=True)

    result = db.get_claims_string_with_property("Tokyo", "populationTotal")
    assert len(result) > 0

    tokyo_lid = db.get_lid("Tokyo")
    result = db.get_claims_string_with_property(tokyo_lid, "populationTotal")
    assert len(result) > 0

    subdivision_lid = db.get_lid("populationTotal")
    result = db.get_claims_string_with_property(tokyo_lid, subdivision_lid)
    assert len(result) > 0


def test_get_claims_time():
    db = DBDBpedia(readonly=True)
    assert len(db.get_claims_time("Bob Preston  CareerStation  7")) > 0

    lid = db.get_lid("Bob Preston  CareerStation  7")
    assert len(db.get_claims_time(lid)) > 0


def test_get_claims_time_with_property():
    db = DBDBpedia(readonly=True)

    result = db.get_claims_time_with_property("Bob Preston  CareerStation  7", "years")
    assert len(result) > 0

    tokyo_lid = db.get_lid("Bob Preston  CareerStation  7")
    result = db.get_claims_time_with_property(tokyo_lid, "years")
    assert len(result) > 0

    subdivision_lid = db.get_lid("years")
    result = db.get_claims_time_with_property(tokyo_lid, subdivision_lid)
    assert len(result) > 0


def test_get_claims_quantity():
    db = DBDBpedia(readonly=True)
    assert len(db.get_claims_quantity("Tokyo")) > 0

    tokyo_lid = db.get_lid("Tokyo")
    assert len(db.get_claims_quantity(tokyo_lid)) > 0


def test_get_claims_quantity_with_property():
    db = DBDBpedia(readonly=True)

    result = db.get_claims_quantity_with_property("Tokyo", "populationTotal")
    assert len(result) > 0

    tokyo_lid = db.get_lid("Tokyo")
    result = db.get_claims_quantity_with_property(tokyo_lid, "populationTotal")
    assert len(result) > 0

    subdivision_lid = db.get_lid("populationTotal")
    result = db.get_claims_quantity_with_property(tokyo_lid, subdivision_lid)
    assert len(result) > 0


def test_get_aliases_en():
    db = DBDBpedia(readonly=True)

    assert len(db.get_aliases_en("Tokyo")) > 64

    tokyo_lid = db.get_lid("Tokyo")
    assert len(db.get_aliases_en(tokyo_lid)) > 64


def test_get_aliases_all():
    db = DBDBpedia(readonly=True)

    assert len(db.get_aliases_all("Tokyo")) > 0

    tokyo_lid = db.get_lid("Tokyo")
    assert len(db.get_aliases_all(tokyo_lid)) > 0


def test_get_item():
    db = DBDBpedia(readonly=True)

    result = db.get_item("Tokyo")
    assert "label" in result and "desc" in result and "types_specific" in result


def test_get_stats():
    db = DBDBpedia(readonly=True)

    stats = db.stats()
    assert (
        "directory" in stats
        and "size" in stats
        and "items" in stats
        and "datatype" in stats
        and "head" in stats
    )


def test_get_wikipedia():
    db = DBDBpedia(readonly=True)

    assert db.get_wikipedia("Tokyo") == "Tokyo"
    assert db.get_wikipedia(db.get_lid("ToKyo")) == "Tokyo"


def test_get_wikidata():
    db = DBDBpedia(readonly=True)

    assert db.get_wikidata("Ship") == "Q11446"
    assert db.get_wikidata(db.get_lid("Ship")) == "Q11446"
