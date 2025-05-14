import logging

import pytest
import yaml
from suthing import FileHandle

from graphcast.architecture.actor import ActorWrapper
from graphcast.architecture.onto import ActionContext
from graphcast.architecture.vertex import VertexConfig

logger = logging.getLogger(__name__)


@pytest.fixture()
def vc_openalex():
    tc = yaml.safe_load("""
    vertices:
    -   name: author
        dbname: authors
        fields:
        -   _key
        -   display_name
        -   updated_date
        -   created_date
        indexes:
        -   fields:
            -   _key
    -   name: institution
        dbname: institutions
        fields:
        -   _key
        -   display_name
        -   country
        -   type
        -   created_date
        -   updated_date
        indexes:
        -   fields:
            -   _key
    """)
    return VertexConfig.from_dict(tc)


@pytest.fixture()
def sample_openalex():
    sample = FileHandle.load("test/data/json/openalex.authors.json")
    return sample


@pytest.fixture()
def resource_with_weights():
    an = yaml.safe_load("""
    -   vertex: author
    -   name: keep_suffix_id
        foo: split_keep_part
        module: graphcast.util.transform
        params:
            sep: "/"
            keep: -1
        input:
        -   id
        output:
        -   _key
    -   key: last_known_institution
        apply:
        -   vertex: institution   
        -   name: keep_suffix_id
    -   source: author
        target: institution
        weights:
            source_fields:
            -   updated_date
            -   created_date
    """)
    return an


def test_act_openalex(resource_with_weights, vc_openalex, sample_openalex):
    ctx = ActionContext()
    anw = ActorWrapper(*resource_with_weights)
    anw.finish_init(vertex_config=vc_openalex, transforms={})
    ctx = anw(ctx, doc=sample_openalex)
    edge = ctx.acc_global[("author", "institution", None)][0]
    del edge["__source"]
    del edge["__target"]
    assert ctx.acc_global[("author", "institution", None)][0] == {
        "updated_date": "2023-06-08",
        "created_date": "2023-06-08",
    }
