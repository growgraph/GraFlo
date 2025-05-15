# GraphCast <img src="docs/assets/favicon.ico" alt="suthing logo" style="height: 32px; width:32px;"/>

A framework for transforming **tabular** data (CSV) and **hierarchical** data (JSON, XML) into property graphs and ingesting them into graph databases (ArangoDB, Neo4j).

![Python](https://img.shields.io/badge/python-3.11-blue.svg) 
[![License: BSL](https://img.shields.io/badge/license-BSL--1.1-green)](https://github.com/growgraph/graphcast/blob/main/LICENSE)
[![pre-commit](https://github.com/growgraph/graphcast/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/growgraph/graphcast/actions/workflows/pre-commit.yml)
[![PyPI version](https://badge.fury.io/py/graphcast.svg)](https://badge.fury.io/py/graphcast)

<!-- [![pytest](https://github.com/growgraph/graphcast/actions/workflows/pytest.yml/badge.svg)](https://github.com/growgraph/graphcast/actions/workflows/pytest.yml) -->


## Features

- **Data Transformation Meta-language**: Describe how your data becomes a property graph with a support of numerous features:
    - compound index for vertices and nodes
    - blank 
    - edge constraints
    - egde properties
    - filter vertices and edges
- **Parallel processing**: Use as many cores as you have
- **Database support**: Ingest into ArangoDB and Neo4j using the same API (database agnostic)
- **GraphCast Server**: Use a dockerized server to populate your databases

<!-- Transparent and Composable configuration, with a clear isolation of transformation from DB settings (indexing). -->

## Documentation
Full documentation is available at: [growgraph.github.io/graphcast](https://growgraph.github.io/graphcast)

## Installation

```bash
pip install graphcast
```

## Usage Examples

### Simple ingest

```python
from suthing import ConfigFactory, FileHandle

from graphcast import Schema, Caster, Patterns


schema = Schema.from_dict(FileHandle.load(schema_path))

conn_conf = ConfigFactory.create_config(db_config_path)

if resource_pattern_config_path is not None:
    patterns = Patterns.from_dict(
        FileHandle.load(resource_pattern_config_path)
    )
else:
    patterns = Patterns()

schema.fetch_resource()

caster = Caster(
    schema,
    n_cores=n_cores,
    n_threads=n_threads,
)

caster.ingest_files(
    path=source_path,
    batch_size=batch_size,
    conn_conf=conn_conf,
    patterns=patterns,
    init_only=init_only,
)
```


## Development

To install requirements

```shell
git clone git@github.com:growgraph/graphcast.git && cd graphcast
uv sync --dev
```


### Tests

#### Test databases
Spin up Arango from [arango docker folder](./docker/arango) by

```shell
docker-compose --env-file .env up arango
```

and Neo4j from [neo4j docker folder](./docker/arango) by

```shell
docker-compose --env-file .env up neo4j
```

To run unit tests

```shell
pytest test
```


## Requirements

- Python 3.11+
- python-arango

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.