import pytest


@pytest.fixture()
def merge_fixture():
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
def merge_result():
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
