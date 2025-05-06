import logging
from collections import defaultdict

import pytest
import yaml
from suthing import FileHandle

from graphcast.architecture.action_node import (
    ActionNodeWrapper,
    DescendNode,
    Edge,
    Transform,
)
from graphcast.architecture.onto import (
    GraphEntity,
)
from graphcast.architecture.vertex import VertexConfig

logger = logging.getLogger(__name__)


@pytest.fixture()
def oa_vc():
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
        -   unique: false
            type: fulltext
            fields:
            -   display_name
        -   unique: false
            fields:
            -   level
        -   unique: false
            fields:
            -   updated_date
        -   unique: false
            fields:
            -   created_date
        -   unique: false
            fields:
            -   wikidata
            -   created_date
    -   name: funder
        dbname: funders
        fields:
        -   _key
        -   display_name
        -   crossref
        -   doi
        -   country_code
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
    -   name: publisher
        dbname: publishers
        fields:
        -   _key
        -   country_codes
        -   display_name
        -   title
        -   hierarchy_level
        -   created_date
        -   data_source
        indexes:
        -   fields:
            -   _key
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
def action_node_descend():
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
def openalex_works_doc():
    an = FileHandle.load("test/data/json/openalex.works.json")
    return an


@pytest.fixture()
def openalex_works_schema():
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
    # -   key: authorships
    #     apply:
    #     -   key: author
    #         apply:
    #         -   vertex: author
    #         -   name: keep_suffix_id
    #     -   key: institutions
    #         apply:
    #         -   vertex: institution
    #         -   name: keep_suffix_id
    # -   key: locations
    #     apply:
    #     -   key: source
    #         apply:
    #         -   vertex: source
    #         -   name: keep_suffix_id
    #     -   source: publisher
    #         target: source
    #         relation: contains
    # -   source: source
    #     target: work
    #     relation: contains
    #     target_discriminant: _top_level
    # -   source: author
    #     target: work
    #     target_discriminant: _top_level
    #     weights:
    #         direct:
    #         -   author_position
    -   key: referenced_works
        apply:
        -   vertex: work
        -   name: keep_suffix_id
    -   source: work
        target: work
        source_discriminant: _top_level
    """)
    return an


def test_descend(action_node_descend):
    anw = ActionNodeWrapper(**action_node_descend)
    assert isinstance(anw.action_node, DescendNode)
    assert len(anw.action_node.descendants) == 2
    assert isinstance(anw.action_node.descendants[-1].action_node, DescendNode)


def test_edge(action_node_edge):
    anw = ActionNodeWrapper(**action_node_edge)
    assert isinstance(anw.action_node, Edge)


def test_transform(action_node_transform):
    anw = ActionNodeWrapper(**action_node_transform)
    assert isinstance(anw.action_node, Transform)


def test_actio_node_wrapper_openalex(openalex_works_schema, openalex_works_doc, oa_vc):
    acc: defaultdict[GraphEntity, list] = defaultdict(list)
    anw = ActionNodeWrapper(*openalex_works_schema)
    _ = anw((openalex_works_doc, acc), oa_vc)
    assert True
