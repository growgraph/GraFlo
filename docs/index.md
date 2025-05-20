# GraphCast <img src="https://raw.githubusercontent.com/growgraph/graphcast/main/docs/assets/favicon.ico" alt="graphcast logo" style="height: 32px; width:32px;"/>

GraphCast is a framework for transforming **tabular** data (CSV) and **hierarchical** data (JSON, XML) into property graphs and ingesting them into graph databases (ArangoDB, Neo4j).

![Python](https://img.shields.io/badge/python-3.11-blue.svg) 
[![PyPI version](https://badge.fury.io/py/graphcast.svg)](https://badge.fury.io/py/graphcast)
[![PyPI Downloads](https://static.pepy.tech/badge/graphcast)](https://pepy.tech/projects/graphcast)
[![License: BSL](https://img.shields.io/badge/license-BSL--1.1-green)](https://github.com/growgraph/graphcast/blob/main/LICENSE)
[![pre-commit](https://github.com/growgraph/graphcast/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/growgraph/graphcast/actions/workflows/pre-commit.yml)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.15446131.svg)]( https://doi.org/10.5281/zenodo.15446131)



<!-- [![pytest](https://github.com/growgraph/graphcast/actions/workflows/pytest.yml/badge.svg)](https://github.com/growgraph/graphcast/actions/workflows/pytest.yml) -->


## Core Concepts

### Property Graphs
GraphCast works with property graphs, which consist of:

- **Vertices**: Nodes with properties and optional unique identifiers
- **Edges**: Relationships between vertices with their own properties
- **Properties**: Both vertices and edges may have properties

### Schema
The Schema defines how your data should be transformed into a graph and contains:

- **Vertex Definitions**: Specify vertex types, their properties, and unique identifiers
- **Edge Definitions**: Define relationships between vertices and their properties
- **Resource Mapping**: describe how data sources map to vertices and edges
- **Transforms**: Modify data during the casting process

### Resources
Resources are your data sources that can be:

- **Table-like**: CSV files, database tables
- **JSON-like**: JSON files, nested data structures

## Key Features

- **Graph Transformation Meta-language**: A powerful declarative language to describe how your data becomes a property graph:
    - Define vertex and edge structures
    - Set compound indexes for vertices and edges
    - Use blank vertices for complex relationships
    - Specify edge constraints and properties
    - Apply advanced filtering and transformations
- **Parallel Processing**: Efficient processing with multi-threading
- **Database Integration**: Seamless integration with Neo4j and ArangoDB
- **Advanced Filtering**: Powerful filtering capabilities for data transformation
- **Blank Node Support**: Create intermediate vertices for complex relationships

## Quick Links

- [Installation](getting_started/installation.md)
- [Quick Start Guide](getting_started/quickstart.md)
- [API Reference](reference/index.md)
- [Examples](examples/index.md)

## Use Cases

- **Data Migration**: Transform relational data into graph structures
- **Knowledge Graphs**: Build complex knowledge representations
- **Data Integration**: Combine multiple data sources into a unified graph

## Requirements

- Python 3.11 or higher
- Graph database (Neo4j or ArangoDB) for storage
- Dependencies as specified in pyproject.toml

## Contributing

We welcome contributions! Please check out our [Contributing Guide](contributing.md) for details on how to get started.
