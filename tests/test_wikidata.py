import json

from kgdb.resources.db.db_wikidata import DBWikidata


def test_get_redirect():
    db = DBWikidata(readonly=True)

    assert db.get_redirect("Q11199581", decode_value=True) == "Q1490"

    tokyo_lid = db.get_lid("Q1490")
    assert db.get_redirect("Q11199581", decode_value=False) == tokyo_lid

    redirect_lid = db.get_lid("Q11199581")
    assert db.get_redirect(redirect_lid, decode_value=False) == tokyo_lid


def test_get_redirect_of():
    db = DBWikidata(readonly=True)

    assert len(db.get_redirect_of("Q1490")) >= 5

    assert len(db.get_redirect_of("Q1490", decode_value=False)) >= 5

    tokyo_lid = db.get_lid("Q1490")
    assert len(db.get_redirect_of(tokyo_lid)) >= 5

    assert len(db.get_redirect_of(tokyo_lid, decode_value=False)) >= 5


def test_get_wikipedia():
    db = DBWikidata(readonly=True)

    assert db.get_wikipedia("Q17") == "Japan"
    assert db.get_wikipedia(db.get_lid("Q17")) == "Japan"


def test_get_dbpedia():
    db = DBWikidata(readonly=True)

    assert db.get_dbpedia("Q846570") == "Us people"
    assert db.get_dbpedia(db.get_lid("Q846570")) == "Us people"


def test_get_label():
    db = DBWikidata(readonly=True)
    assert db.get_label("Q1490") == "Tokyo"

    tokyo_lid = db.get_lid("Q1490")
    assert db.get_label(tokyo_lid) == "Tokyo"


def test_get_descriptions():
    db = DBWikidata(readonly=True)
    # Check describtion available
    assert db.get_descriptions("Q1490") is not None

    tokyo_lid = db.get_lid("Q1490")
    assert db.get_descriptions(tokyo_lid) is not None


def test_get_types_specific():
    db = DBWikidata(readonly=True)
    assert db.get_types_specific("Q1490") == "City"

    tokyo_lid = db.get_lid("Q1490")
    assert db.get_types_specific(tokyo_lid) == "City"

    city_lid = db.get_lid("City")
    assert db.get_types_specific(tokyo_lid, decode_value=False) == city_lid


def test_get_types_transitive():
    db = DBWikidata(readonly=True)

    assert len(db.get_types_transitive("Q1490")) > 0

    tokyo_lid = db.get_lid("Q1490")
    assert len(db.get_types_transitive(tokyo_lid)) > 0

    assert len(db.get_types_transitive(tokyo_lid, decode_value=False)) > 0


def test_get_claims_entity():
    db = DBWikidata(readonly=True)
    assert len(db.get_claims_entity("Q1490", decode_value=True)) > 0

    assert len(db.get_claims_entity("Q1490", decode_value=False)) > 0

    tokyo_lid = db.get_lid("Q1490")
    assert len(db.get_claims_entity(tokyo_lid, decode_value=True)) > 0


def test_get_claims_entity_with_property():
    db = DBWikidata(readonly=True)

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
    db = DBWikidata(readonly=True)
    assert len(db.get_claims_string("Q1490")) > 0

    tokyo_lid = db.get_lid("Q1490")
    assert len(db.get_claims_string(tokyo_lid)) > 0


def test_get_claims_string_with_property():
    db = DBWikidata(readonly=True)

    result = db.get_claims_string_with_property("Q1490", "populationTotal")
    assert len(result) > 0

    tokyo_lid = db.get_lid("Q1490")
    result = db.get_claims_string_with_property(tokyo_lid, "populationTotal")
    assert len(result) > 0

    subdivision_lid = db.get_lid("populationTotal")
    result = db.get_claims_string_with_property(tokyo_lid, subdivision_lid)
    assert len(result) > 0


def test_get_claims_time():
    db = DBWikidata(readonly=True)
    assert len(db.get_claims_time("Bob Preston  CareerStation  7")) > 0

    lid = db.get_lid("Bob Preston  CareerStation  7")
    assert len(db.get_claims_time(lid)) > 0


def test_get_claims_time_with_property():
    db = DBWikidata(readonly=True)

    result = db.get_claims_time_with_property("Bob Preston  CareerStation  7", "years")
    assert len(result) > 0

    tokyo_lid = db.get_lid("Bob Preston  CareerStation  7")
    result = db.get_claims_time_with_property(tokyo_lid, "years")
    assert len(result) > 0

    subdivision_lid = db.get_lid("years")
    result = db.get_claims_time_with_property(tokyo_lid, subdivision_lid)
    assert len(result) > 0


def test_get_claims_quantity():
    db = DBWikidata(readonly=True)
    assert len(db.get_claims_quantity("Q1490")) > 0

    tokyo_lid = db.get_lid("Q1490")
    assert len(db.get_claims_quantity(tokyo_lid)) > 0


def test_get_claims_quantity_with_property():
    db = DBWikidata(readonly=True)

    result = db.get_claims_quantity_with_property("Q1490", "populationTotal")
    assert len(result) > 0

    tokyo_lid = db.get_lid("Q1490")
    result = db.get_claims_quantity_with_property(tokyo_lid, "populationTotal")
    assert len(result) > 0

    subdivision_lid = db.get_lid("populationTotal")
    result = db.get_claims_quantity_with_property(tokyo_lid, subdivision_lid)
    assert len(result) > 0


def test_get_aliases_en():
    db = DBWikidata(readonly=True)

    assert len(db.get_aliases_en("Q1490")) > 64

    tokyo_lid = db.get_lid("Q1490")
    assert len(db.get_aliases_en(tokyo_lid)) > 64


def test_get_aliases_all():
    db = DBWikidata(readonly=True)

    assert len(db.get_aliases_all("Q1490")) > 0

    tokyo_lid = db.get_lid("Q1490")
    assert len(db.get_aliases_all(tokyo_lid)) > 0


def test_get_item():
    db = DBWikidata(readonly=True)

    result = db.get_item("Q1490")
    assert "label" in result and "desc" in result and "types_specific" in result


def test_get_stats():
    db = DBWikidata(readonly=True)

    stats = db.stats()
    assert (
        "directory" in stats
        and "size" in stats
        and "items" in stats
        and "datatype" in stats
        and "head" in stats
    )
