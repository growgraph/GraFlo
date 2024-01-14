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
            module: graph_cast.util.transform
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
        collection_name_suffix: aux
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
        collection_name_suffix: aux
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
def vertex_config_ibes():
    vc = yaml.safe_load(
        """
        blanks:
        -   publication
        vertices:
            -
                name: publication
                dbname: publications
                fields:
                -   datetime_review
                -   datetime_announce
                indexes:
                -   type: hash
                    unique: false
                    fields:
                    -   datetime_review
                -   type: hash
                    unique: false
                    fields:
                    -   datetime_announce
            -   
                name: ticker
                dbname: tickers
                fields:
                -   cusip
                -   cname
                -   oftic
                indexes:
                -
                    fields:
                    -   cusip
                    -   cname
                    -   oftic
            - 
                name: agency
                dbname: agencies
                fields:
                -   aname
                indexes:
                -
                    fields:                
                    -   aname
            -
                name: analyst
                dbname: analysts
                fields:
                -   last_name
                -   initial
                indexes:
                -
                    fields:
                    -   last_name
                    -   initial
            -
                name: recommendation
                dbname: recommendations
                fields:
                -   erec
                -   etext
                -   irec
                -   itext
                indexes:
                -
                    fields:
                    -   irec
    """
    )
    return vc


@pytest.fixture()
def row_resource_ibes():
    tc = yaml.safe_load(
        """
        type: ibes
        encoding: ISO-8859-1
        transforms:
        -   foo: parse_date_ibes
            module: graph_cast.util.transform
            input:
            -   ANNDATS
            -   ANNTIMS
            output:
            -   datetime_announce
        -   foo: parse_date_ibes
            module: graph_cast.util.transform
            input:
            -   REVDATS
            -   REVTIMS
            output:
            -   datetime_review
        -   foo: cast_ibes_analyst
            module: graph_cast.util.transform
            input:
            -   ANALYST
            output:
            -   last_name
            -   initial
        -   map:
                CUSIP: cusip
                CNAME: cname
                OFTIC: oftic
        -   map:
                ESTIMID: aname
        -   map:
                ERECCD: erec
                ETEXT: etext
                IRECCD: irec
                ITEXT: itext
    """
    )
    return tc


@pytest.fixture()
def mapper_node_a():
    mn = yaml.safe_load(
        """
        type: vertex
        name: date
        transforms:
        -   foo: parse_date_standard
            module: graph_cast.util.transform
            input:
            -   '@sortdate'
            output:
            -   year
            -   month
            -   day
    """
    )
    return mn


@pytest.fixture()
def mapper_node_edge():
    mn = yaml.safe_load(
        """
        type: edge
        edge:
            how: all
            source: mention
            target: entity
    """
    )
    return mn


@pytest.fixture()
def mapper_node_tree():
    mn = yaml.safe_load(
        """
        type: descend
        key: map_mention_entity
        children:
        -   children:
            -   type: edge
                how: all
                edge:
                    source: mention
                    target: entity
            -   type: descend
                key: entity
                children:
                -   type: vertex
                    name: entity
                    map:
                        hash: _key
            -   type: descend
                key: mention
                children:
                -   type: vertex
                    name: mention
                    map:
                        hash: _key
    """
    )
    return mn


@pytest.fixture()
def mapper_node_edge_weight_config():
    mn = yaml.safe_load(
        """
        type: edge
        edge:
            how: all
            source: mention
            target: entity
            weights:
                source_fields:
                - a
                target_fields:
                - a
                vertices:
                -
                    fields:
                    -   a
    """
    )
    return mn


@pytest.fixture()
def mapper_value():
    mn = yaml.safe_load(
        """
        key: ids
        children:
        -   key: mag
            children:
            -   type: value
                name: concept
        -   key: wikidata
            children:
            -   type: value
                name: concept
                transforms:
                -   foo: split_keep_part
                    module: graph_cast.util.transform
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
