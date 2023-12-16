import pytest
import yaml


@pytest.fixture()
def yaml_vertex_pub():
    tc = yaml.safe_load("""
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
    """)
    return tc
