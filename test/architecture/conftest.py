import pytest
import yaml
from suthing import FileHandle

from graphcast.architecture import VertexConfig


@pytest.fixture()
def vertex_pub():
    tc = yaml.safe_load(
        """
        name: publication
        dbname: publications
        fields:
        -   arxiv
        -   doi
        -   created
        -   data_source
        indexes:
        -   fields:
            -   arxiv
            -   doi
        -   unique: false
            fields:
            -   created
        -   unique: false
            fields:
            -   created
        filters:
        -   OR:
            -   IF_THEN:
                -   field: name
                    foo: __eq__
                    value: Open
                -   field: value
                    foo: __gt__
                    value: 0
            -   IF_THEN:
                -   field: name
                    foo: __eq__
                    value: Close
                -   field: value
                    foo: __gt__
                    value: 0
        transforms:
        -   foo: cast_ibes_analyst
            module: graphcast.util.transform
            input:
            -   ANALYST
            output:
            -   last_name
            -   initial
    """
    )
    return tc


@pytest.fixture()
def vertex_helper():
    tc = yaml.safe_load(
        """
        name: analyst
    """
    )
    return tc


@pytest.fixture()
def vertex_helper_b():
    tc = yaml.safe_load(
        """
            fields:
            -   datetime_review
            -   datetime_announce
    """
    )
    return tc


@pytest.fixture()
def edge_with_weights():
    tc = yaml.safe_load(
        """
        source: analyst
        target: agency
        weights:
            vertices:
                -   
                    name: ticker
                    fields:
                        - cusip
                -
                    fields:
                        - datetime_review
                        - datetime_announce
    """
    )
    return tc


@pytest.fixture()
def edge_indexes():
    tc = yaml.safe_load(
        """
        source: entity
        target: entity
        purpose: aux
        indexes:
        -   
            fields:
            -   start_date
            -   end_date
        -   
            fields:
            -   spec
    """
    )
    return tc


@pytest.fixture()
def edge_with_vertex_indexes():
    tc = yaml.safe_load(
        """
        source: entity
        target: entity
        indexes:
        -   name: publication
        -   exclude_edge_endpoints: true
            unique: false
            name: publication
            fields:
            -   _key
    """
    )
    return tc


@pytest.fixture()
def index_a():
    tc = yaml.safe_load(
        """
    fields:
        -   start_date
        -   end_date
    """
    )
    return tc


@pytest.fixture()
def vertex_config_kg():
    vc = yaml.safe_load(
        """
    vertices:
    -   name: publication
        dbname: publications
        fields:
        -   arxiv
        -   doi
        -   created
        -   data_source
        indexes:
        -   fields:
            -   arxiv
            -   doi
        -   unique: false
            fields:
            -   created
        -   unique: false
            fields:
            -   created
    -   name: entity
        dbname: entities
        fields:
        -   linker_type
        -   ent_db_type
        -   id
        -   ent_type
        -   original_form
        -   description
        indexes:
        -   fields:
            -   id
            -   ent_type
    -   name: mention
        dbname: mentions
        fields:
        -   text
        indexes:
        -   unique: false
            fields:
            -   text
    """
    )
    return vc


@pytest.fixture()
def edge_config_kg():
    tc = yaml.safe_load(
        """
    edges:
    -   source: entity
        target: entity
        index:
        -   name: publication
            fields:
            -   _key
        -   exclude_edge_end_vertices: true
            unique: false
            fields:
            -   publication@_key
    -   source: entity
        target: entity
        purpose: aux
        index:
        -   fields:
            -   start_date
            -   end_date
        -   fields:
            -   spec
    -   source: mention
        target: entity
        index:
        -   name: publication
            fields:
            -   _key
    """
    )
    return tc


@pytest.fixture()
def resource_concept():
    mn = yaml.safe_load(
        """
        -   vertex: concept
        -   foo: split_keep_part
            module: graphcast.util.transform
            params:
                sep: "/"
                keep: -1
            input:
            -   wikidata
            output:
            -   wikidata
    """
    )
    return mn


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


@pytest.fixture()
def vertex_config_cross():
    tc = yaml.safe_load("""
    vertex_config:
    vertices:
    -   name: person
        fields:
        -   id
        indexes:
        -   fields:
            -   id
    -   name: company
        fields:
        -   id
        indexes:
        -   fields:
            -   id
    """)
    return VertexConfig.from_dict(tc)


@pytest.fixture()
def sample_cross():
    an = yaml.safe_load("""
    -   name: John
        id: Apple
    -   name: Mary
        id: Oracle
    """)
    return an


@pytest.fixture()
def resource_cross():
    an = yaml.safe_load("""
    -   vertex: person
    -   vertex: company 
    -   target_vertex: person
        map:
            name: id
    """)
    return an
