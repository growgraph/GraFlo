import logging

from graphcast.util.merge import merge_doc_basis_closest_preceding

logger = logging.getLogger(__name__)


def test_merge():
    input = [
        {"_key": "8de0b4225b0f31db5784c73f3ea9d8ce73954c61", "_role": "source"},
        {"text": "complex evolutionary history of acochlidia"},
        {"_key": "4275320fcb2ee3c9bb2711b735b265e847256628", "_role": "relation"},
        {"text": "represents"},
        {"_key": "009c700138c1c718b0be5730ff557f8aa3c13b63", "_role": "target"},
        {"text": "small group of panpulmonata"},
    ]

    output_ref = [
        {
            "_key": "8de0b4225b0f31db5784c73f3ea9d8ce73954c61",
            "_role": "source",
            "text": "complex evolutionary history of acochlidia",
        },
        {
            "_key": "4275320fcb2ee3c9bb2711b735b265e847256628",
            "_role": "relation",
            "text": "represents",
        },
        {
            "_key": "009c700138c1c718b0be5730ff557f8aa3c13b63",
            "_role": "target",
            "text": "small group of panpulmonata",
        },
    ]

    output = merge_doc_basis_closest_preceding(input, index_keys=("_key",))
    assert output == output_ref
