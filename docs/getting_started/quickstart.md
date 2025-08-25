# Quick Start Guide

This guide will help you get started with GraphCast by showing you how to transform data into a graph structure.

## Basic Concepts

- GraphCast uses `Caster` class to cast data into a property graph representation and eventually graph database. 
- Class `Schema` encodes the representation of vertices, and edges (relations), the transformations the original data undergoes to become a graph and how data sources are mapped onto graph definition.
- `Resource` class managed.
- In case the data is provided as files, class `Patterns` manages the mapping of the resources to files. 
- `ConfigFactory` is used to created database connections used by `Caster`.

## Basic Example

Here's a simple example of transforming CSV files of two types, `people` and `department` into a graph:

```python
from suthing import ConfigFactory, FileHandle
from graphcast import Caster, Patterns, Schema

schema = Schema.from_dict(FileHandle.load("schema.yaml"))

caster = Caster(schema)

conn_conf = ConfigFactory.create_config(
    {
        "protocol": "http",
        "hostname": "localhost",
        "port": 8535,
        "username": "root",
        "password": "123",
        "database": "_system",
    }
)

patterns = Patterns.from_dict(
    {
        "patterns": {
            "people": {"regex": "^people.*\.csv$"},
            "departments": {"regex": "^dep.*\.csv$"},
        }
    }
)

caster.ingest_files(
    path=".",
    conn_conf=conn_conf,
    patterns=patterns,
)
```

Here `schema` defines the graph and the mapping the sources to vertices and edges (refer to [Schema](concepts/schema) for details on schema and its components).
In `patterns` the keys `"people"` and `"departments"` correspond to source types (`Resource`) from `Schema`, while `"regex"` contain regex patterns to find the corresponding files. 


## Next Steps

- Explore the [API Reference](../reference/index.md) for detailed documentation
- Check out more [Examples](../examples/index.md) for advanced use cases
- Learn main [concepts](../concepts/index.md), such as `Schema` and its constituents 