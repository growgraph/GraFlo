import logging

from graflo.architecture.onto import VertexRep
from graflo.util.merge import merge_doc_basis_closest_preceding

logger = logging.getLogger(__name__)


def test_merge():
    input = [
        VertexRep(
            vertex={
                "_key": "8de0b4225b0f31db5784c73f3ea9d8ce73954c61",
                "_role": "source",
            },
            ctx={"a": 1},
        ),
        VertexRep(
            vertex={"text": "complex evolutionary history of acochlidia"}, ctx={"b": 2}
        ),
        VertexRep(
            vertex={
                "_key": "4275320fcb2ee3c9bb2711b735b265e847256628",
                "_role": "relation",
            },
            ctx={"c": 3},
        ),
        VertexRep(vertex={"text": "represents"}, ctx={"d": 4}),
        VertexRep(
            vertex={
                "_key": "009c700138c1c718b0be5730ff557f8aa3c13b63",
                "_role": "target",
            },
            ctx={"e": 5},
        ),
        VertexRep(vertex={"text": "small group of panpulmonata"}, ctx={"f": 6}),
    ]

    output_ref = [
        VertexRep(
            vertex={
                "_key": "8de0b4225b0f31db5784c73f3ea9d8ce73954c61",
                "_role": "source",
                "text": "complex evolutionary history of acochlidia",
            },
            ctx={"a": 1, "b": 2},
        ),
        VertexRep(
            vertex={
                "_key": "4275320fcb2ee3c9bb2711b735b265e847256628",
                "_role": "relation",
                "text": "represents",
            },
            ctx={"c": 3, "d": 4},
        ),
        VertexRep(
            vertex={
                "_key": "009c700138c1c718b0be5730ff557f8aa3c13b63",
                "_role": "target",
                "text": "small group of panpulmonata",
            },
            ctx={"e": 5, "f": 6},
        ),
    ]

    output = merge_doc_basis_closest_preceding(input, index_keys=("_key",))
    # Compare vertex and ctx dicts for equality
    for o, r in zip(output, output_ref):
        assert o.vertex == r.vertex
        assert o.ctx == r.ctx
