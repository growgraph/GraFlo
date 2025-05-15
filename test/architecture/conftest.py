import pytest
import yaml


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
