import logging
from pathlib import Path

import pytest
import yaml
from suthing import FileHandle

from graphcast.architecture.actor import (
    ActorWrapper,
    DescendActor,
    EdgeActor,
    TransformActor,
)
from graphcast.architecture.edge import EdgeConfig
from graphcast.architecture.onto import ActionContext
from graphcast.architecture.vertex import VertexConfig
from graphcast.plot.plotter import assemble_tree

logger = logging.getLogger(__name__)


@pytest.fixture()
def schema_vc_openalex():
    tc = yaml.safe_load("""
    vertex_config:
    vertices:
    -   name: author
        dbname: authors
        fields:
        -   _key
        -   display_name
        -   updated_date
        indexes:
        -   fields:
            -   _key
        -   unique: false
            type: fulltext
            fields:
            -   display_name
        -   unique: false
            fields:
            -   updated_date
        -   unique: false
            fields:
            -   created_date
    -   name: concept
        dbname: concepts
        fields:
        -   _key
        -   wikidata
        -   display_name
        -   level
        -   mag
        -   created_date
        -   updated_date
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
        -   ror
        -   grid
        -   wikidata
        -   mag
        -   created_date
        -   updated_date
        indexes:
        -   fields:
            -   _key
        -   unique: false
            type: fulltext
            fields:
            -   display_name
        -   unique: false
            fields:
            -   type
    -   name: source
        dbname: sources
        fields:
        -   _key
        -   issn_l
        -   type
        -   display_name
        -   created_date
        -   updated_date
        -   country_code
        indexes:
        -   fields:
            -   _key
        -   fields:
            -   issn_l
    -   name: work
        dbname: works
        fields:
        -   _key
        -   doi
        -   title
        -   created_date
        -   updated_date
        -   publication_date
        -   publication_year
        indexes:
        -   fields:
            -   _key
        -   fields:
            -   doi
    """)
    return VertexConfig.from_dict(tc)


@pytest.fixture()
def resource_descend():
    tc = yaml.safe_load(
        """
        key: publications
        apply:
        - key: abc
          apply:
            name: a
        - vertex: work
        """
    )
    return tc


@pytest.fixture()
def action_node_edge():
    tc = yaml.safe_load(
        """
        source: source
        target: work
        relation: contains
        target_discriminant: _top_level
        """
    )
    return tc


@pytest.fixture()
def action_node_transform():
    an = yaml.safe_load("""
        foo: parse_date_ibes
        module: graphcast.util.transform
        input:
        -   ANNDATS
        -   ANNTIMS
        output:
        -   datetime_announce
    """)
    return an


@pytest.fixture()
def sample_openalex():
    an = FileHandle.load("test/data/json/openalex.works.json")
    return an


def test_descend(resource_descend, schema_vc_openalex):
    anw = ActorWrapper(**resource_descend)
    anw.finish_init(vertex_config=schema_vc_openalex)
    assert isinstance(anw.actor, DescendActor)
    assert len(anw.actor.descendants) == 2
    assert isinstance(anw.actor.descendants[0].actor, DescendActor)
    level, cname, label, edges = anw.fetch_actors(0, [])
    assert len(edges) == 3

    assemble_tree(anw, Path("test/figs/test.pdf"))


def test_edge(action_node_edge, schema_vc_openalex):
    anw = ActorWrapper(**action_node_edge)
    anw.finish_init(
        transforms={}, vertex_config=schema_vc_openalex, edge_config=EdgeConfig()
    )
    assert isinstance(anw.actor, EdgeActor)
    assert anw.actor.edge.target == "work"


def test_transform(action_node_transform, schema_vc_openalex):
    anw = ActorWrapper(**action_node_transform)
    anw.finish_init(vertex_config=schema_vc_openalex)
    assert isinstance(anw.actor, TransformActor)


def test_discriminant_edge(
    resource_openalex_works, schema_vc_openalex, sample_openalex
):
    ctx = ActionContext()
    anw = ActorWrapper(*resource_openalex_works)
    ec = EdgeConfig()
    anw.finish_init(vertex_config=schema_vc_openalex, transforms={}, edge_config=ec)
    assemble_tree(anw, Path("test/figs/discriminate_edge.pdf"))
    ctx = anw(ctx, doc=sample_openalex)
    assert sum(len(v) for v in ctx.acc_vertex["work"].values()) == 6
    assert len(ctx.acc_global[("work", "work", None)]) == 5


def test_mapper_value(resource_concept, schema_vc_openalex):
    test_doc = [{"wikidata": "https://www.wikidata.org/wiki/Q123", "mag": 105794591}]
    anw = ActorWrapper(*resource_concept)
    anw.finish_init(vertex_config=schema_vc_openalex, transforms={})
    ctx = ActionContext()
    ctx = anw(ctx, doc=test_doc)
    assert ctx.acc_v_local["concept"][None][0] == {
        "mag": 105794591,
        "wikidata": "Q123",
    }
    assert len(ctx.acc_v_local) == 1


def test_transform_shortcut(resource_openalex_works, schema_vc_openalex):
    doc = {
        "doi": "https://doi.org/10.1007/978-3-123",
        "id": "https://openalex.org/A123",
    }
    anw = ActorWrapper(*resource_openalex_works)
    transforms = {}
    anw.finish_init(vertex_config=schema_vc_openalex, transforms=transforms)
    ctx = ActionContext()
    ctx = anw(ctx, doc=doc)
    # we are checking acc_vertex because EdgeActor moved it from acc_vertex_local
    assert ctx.acc_vertex["work"]["_top_level"][0] == {
        "_key": "A123",
        "doi": "10.1007/978-3-123",
    }
