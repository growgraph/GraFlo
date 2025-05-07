import logging

import pytest
import yaml
from suthing import FileHandle

from graphcast.architecture.action_node import (
    ActionContext,
    ActionNodeWrapper,
    DescendNode,
    EdgeNode,
    TransformNode,
)
from graphcast.architecture.vertex import VertexConfig

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
        - vertex: b
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


@pytest.fixture()
def resource_openalex_works():
    an = yaml.safe_load("""
    -   vertex: work
        discriminant: _top_level
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
    -   key: referenced_works
        apply:
        -   vertex: work
        -   name: keep_suffix_id
    -   source: work
        target: work
        source_discriminant: _top_level
    """)
    return an


def test_descend(resource_descend, schema_vc_openalex):
    anw = ActionNodeWrapper(**resource_descend)
    anw.finish_init(vertex_config=schema_vc_openalex)
    assert isinstance(anw.action_node, DescendNode)
    assert len(anw.action_node.descendants) == 2
    assert isinstance(anw.action_node.descendants[-1].action_node, DescendNode)


def test_edge(action_node_edge, schema_vc_openalex):
    anw = ActionNodeWrapper(**action_node_edge)
    anw.finish_init(transforms={}, vertex_config=schema_vc_openalex)
    assert isinstance(anw.action_node, EdgeNode)


def test_transform(action_node_transform, schema_vc_openalex):
    anw = ActionNodeWrapper(**action_node_transform)
    anw.finish_init(vertex_config=schema_vc_openalex)
    assert isinstance(anw.action_node, TransformNode)


def test_discriminant_edge(
    resource_openalex_works, schema_vc_openalex, sample_openalex
):
    ctx = ActionContext(doc=sample_openalex)
    anw = ActionNodeWrapper(*resource_openalex_works)
    anw.finish_init(vertex_config=schema_vc_openalex, transforms={})
    _ = anw(ctx)
    assert True
