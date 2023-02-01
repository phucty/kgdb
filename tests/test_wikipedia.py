import json

from kgdb.resources.db.db_wikipedia import DBWikipedia


def test_get_redirect():
    db = DBWikipedia(readonly=True)

    assert db.get_redirect("Tokei", decode_value=True) == "Tokyo"

    tokyo_lid = db.get_lid("Tokyo")
    assert db.get_redirect("Tokei", decode_value=False) == tokyo_lid

    redirect_lid = db.get_lid("Tokei")
    assert db.get_redirect(redirect_lid, decode_value=False) == tokyo_lid


def test_get_redirect_of():
    db = DBWikipedia(readonly=True)

    assert len(db.get_redirect_of("Tokyo")) >= 5

    assert len(db.get_redirect_of("Tokyo", decode_value=False)) >= 5

    tokyo_lid = db.get_lid("Tokyo")
    assert len(db.get_redirect_of(tokyo_lid)) >= 5

    assert len(db.get_redirect_of(tokyo_lid, decode_value=False)) >= 5


def test_get_wikidata():
    db = DBWikipedia(readonly=True)

    assert db.get_wikidata("Tokyo") == "Q7473516"

    tokyo_lid = db.get_lid("Tokyo")
    assert db.get_wikidata(tokyo_lid) == "Q7473516"


def test_get_label():
    db = DBWikipedia(readonly=True)
    assert db.get_label("Q1490") == "Tokyo"

    tokyo_lid = db.get_lid("Q1490")
    assert db.get_label(tokyo_lid) == "Tokyo"


def test_get_descriptions():
    db = DBWikipedia(readonly=True)
    # Check describtion available
    assert db.get_descriptions("Q1490") is not None

    tokyo_lid = db.get_lid("Q1490")
    assert db.get_descriptions(tokyo_lid) is not None


def test_get_types_specific():
    db = DBWikipedia(readonly=True)
    assert db.get_types_specific("Q1490") == "City"

    tokyo_lid = db.get_lid("Q1490")
    assert db.get_types_specific(tokyo_lid) == "City"

    city_lid = db.get_lid("City")
    assert db.get_types_specific(tokyo_lid, decode_value=False) == city_lid


def test_get_types_transitive():
    db = DBWikipedia(readonly=True)

    assert len(db.get_types_transitive("Q1490")) > 0

    tokyo_lid = db.get_lid("Q1490")
    assert len(db.get_types_transitive(tokyo_lid)) > 0

    assert len(db.get_types_transitive(tokyo_lid, decode_value=False)) > 0


def test_get_claims_entity():
    db = DBWikipedia(readonly=True)
    assert len(db.get_claims_entity("Q1490", decode_value=True)) > 0

    assert len(db.get_claims_entity("Q1490", decode_value=False)) > 0

    tokyo_lid = db.get_lid("Q1490")
    assert len(db.get_claims_entity(tokyo_lid, decode_value=True)) > 0


def test_get_claims_entity_with_property():
    db = DBWikipedia(readonly=True)

    result = db.get_claims_entity_with_property(
        "Q1490", "subdivision", decode_value=True
    )
    assert len(result) > 0

    result = db.get_claims_entity_with_property(
        "Q1490", "subdivision", decode_value=False
    )
    assert len(result) > 0

    tokyo_lid = db.get_lid("Q1490")
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
    db = DBWikipedia(readonly=True)
    assert len(db.get_claims_string("Q1490")) > 0

    tokyo_lid = db.get_lid("Q1490")
    assert len(db.get_claims_string(tokyo_lid)) > 0


def test_get_claims_string_with_property():
    db = DBWikipedia(readonly=True)

    result = db.get_claims_string_with_property("Q1490", "populationTotal")
    assert len(result) > 0

    tokyo_lid = db.get_lid("Q1490")
    result = db.get_claims_string_with_property(tokyo_lid, "populationTotal")
    assert len(result) > 0

    subdivision_lid = db.get_lid("populationTotal")
    result = db.get_claims_string_with_property(tokyo_lid, subdivision_lid)
    assert len(result) > 0


def test_get_claims_time():
    db = DBWikipedia(readonly=True)
    assert len(db.get_claims_time("Bob Preston  CareerStation  7")) > 0

    lid = db.get_lid("Bob Preston  CareerStation  7")
    assert len(db.get_claims_time(lid)) > 0


def test_get_claims_time_with_property():
    db = DBWikipedia(readonly=True)

    result = db.get_claims_time_with_property("Bob Preston  CareerStation  7", "years")
    assert len(result) > 0

    tokyo_lid = db.get_lid("Bob Preston  CareerStation  7")
    result = db.get_claims_time_with_property(tokyo_lid, "years")
    assert len(result) > 0

    subdivision_lid = db.get_lid("years")
    result = db.get_claims_time_with_property(tokyo_lid, subdivision_lid)
    assert len(result) > 0


def test_get_claims_quantity():
    db = DBWikipedia(readonly=True)
    assert len(db.get_claims_quantity("Q1490")) > 0

    tokyo_lid = db.get_lid("Q1490")
    assert len(db.get_claims_quantity(tokyo_lid)) > 0


def test_get_claims_quantity_with_property():
    db = DBWikipedia(readonly=True)

    result = db.get_claims_quantity_with_property("Q1490", "populationTotal")
    assert len(result) > 0

    tokyo_lid = db.get_lid("Q1490")
    result = db.get_claims_quantity_with_property(tokyo_lid, "populationTotal")
    assert len(result) > 0

    subdivision_lid = db.get_lid("populationTotal")
    result = db.get_claims_quantity_with_property(tokyo_lid, subdivision_lid)
    assert len(result) > 0


def test_get_aliases_en():
    db = DBWikipedia(readonly=True)

    assert len(db.get_aliases_en("Q1490")) > 64

    tokyo_lid = db.get_lid("Q1490")
    assert len(db.get_aliases_en(tokyo_lid)) > 64


def test_get_aliases_all():
    db = DBWikipedia(readonly=True)

    assert len(db.get_aliases_all("Q1490")) > 0

    tokyo_lid = db.get_lid("Q1490")
    assert len(db.get_aliases_all(tokyo_lid)) > 0


def test_get_item():
    db = DBWikipedia(readonly=True)

    result = db.get_item("Q1490")
    assert "label" in result and "desc" in result and "types_specific" in result


def test_get_stats():
    db = DBWikipedia(readonly=True)

    stats = db.stats()
    assert (
        "directory" in stats
        and "size" in stats
        and "items" in stats
        and "datatype" in stats
        and "head" in stats
    )
