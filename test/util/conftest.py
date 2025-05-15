import pytest


@pytest.fixture
def docs_simple():
    return [{"id": 0, "a": 2}, {"id": 0, "b": 1}]


@pytest.fixture
def docs_simple_two_doc():
    return [{"id": 0, "a": 2}, {"id": 1, "b": 1}]


@pytest.fixture()
def merge_input_with_discriminant():
    return [
        {
            "wikidata": "Q775007",
            "_key": "C105794591",
            "created_date": "2016-06-24",
            "display_name": "Walsh diagram",
            "level": 4,
            "updated_date": "2023-06-07",
            "__discriminant_key": "_top_level",
        },
        {"mag": 105794591},
        {"wikidata": "Q775007"},
        {
            "wikidata": "Q725417",
            "_key": "C139358910",
            "display_name": "Molecular orbital",
            "level": 3,
        },
        {
            "wikidata": "Q11369",
            "_key": "C32909587",
            "display_name": "Molecule",
            "level": 2,
        },
        {
            "wikidata": "Q11351",
            "_key": "C178790620",
            "display_name": "Organic chemistry",
            "level": 1,
        },
        {
            "wikidata": "Q944",
            "_key": "C62520636",
            "display_name": "Quantum mechanics",
            "level": 1,
        },
        {
            "wikidata": "Q2329",
            "_key": "C185592680",
            "display_name": "Chemistry",
            "level": 0,
        },
        {
            "wikidata": "Q413",
            "_key": "C121332964",
            "display_name": "Physics",
            "level": 0,
        },
    ]


@pytest.fixture()
def merge_input_no_disc():
    return [
        {"crossref": 501100022272, "doi": "10.13039/501100022272"},
        {
            "_key": "F4320337236",
            "country_code": "AU",
            "created_date": "2023-02-13",
            "display_name": "Graduate School of Health, University of Technology Sydney",
            "updated_date": "2023-06-09T19:18:16.597411",
        },
    ]


@pytest.fixture()
def merge_output():
    return [
        {
            "wikidata": "Q775007",
            "_key": "C105794591",
            "created_date": "2016-06-24",
            "display_name": "Walsh diagram",
            "level": 4,
            "updated_date": "2023-06-07",
            "__discriminant_key": "_top_level",
            "mag": 105794591,
        },
        {
            "wikidata": "Q413",
            "_key": "C121332964",
            "display_name": "Physics",
            "level": 0,
        },
        {
            "wikidata": "Q725417",
            "_key": "C139358910",
            "display_name": "Molecular orbital",
            "level": 3,
        },
        {
            "wikidata": "Q11351",
            "_key": "C178790620",
            "display_name": "Organic chemistry",
            "level": 1,
        },
        {
            "wikidata": "Q2329",
            "_key": "C185592680",
            "display_name": "Chemistry",
            "level": 0,
        },
        {
            "wikidata": "Q11369",
            "_key": "C32909587",
            "display_name": "Molecule",
            "level": 2,
        },
        {
            "wikidata": "Q944",
            "_key": "C62520636",
            "display_name": "Quantum mechanics",
            "level": 1,
        },
    ]


@pytest.fixture()
def merge_output_no_disc():
    return [
        {
            "crossref": 501100022272,
            "doi": "10.13039/501100022272",
            "_key": "F4320337236",
            "country_code": "AU",
            "created_date": "2023-02-13",
            "display_name": "Graduate School of Health, University of Technology Sydney",
            "updated_date": "2023-06-09T19:18:16.597411",
        }
    ]
