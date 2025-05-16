# Installation

## Prerequisites

- Python 3.11+
- A graph database (Neo4j or ArangoDB) if you plan to use database features

## Installation Methods

### Using pip

```bash
pip install graphcast
```

### Using uv (recommended)

```bash
uv add graphcast
```

### From Source

1. Clone the repository:
```bash
git clone https://github.com/growgraph/graphcast.git
cd graphcast
```

2. Install with development dependencies:
```bash
uv sync --group dev
```

## Optional Dependencies

GraphCast has some optional dependencies that can be installed based on your needs.
In order to be able to generate schema visualizations, add graphviz deps (you will need `graphviz` package installed on your computer, e.g. `apt install graphviz-dev`)

```bash
pip install graphcast[graphviz]
```

## Verifying Installation

To verify your installation, you can run:

```python
import graphcast
print(graphcast.__version__)
```


## Spinning up databases

Instructions on how spin up `ArangoDB` and `neo4j` as docker images using `docker compose` are provided here [github.com/growgraph/graphcast/docker](https://github.com/growgraph/graphcast/tree/main/docker) 

## Configuration

After installation, you may need to configure your graph database connection. See the [Quick Start Guide](quickstart.md) for details on setting up your environment.

For more detailed troubleshooting, refer to the [API Reference](reference/index.md) or open an issue on GitHub. 