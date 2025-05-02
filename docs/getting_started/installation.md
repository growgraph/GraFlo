# Installation

## Prerequisites

Before installing GraphCast, ensure you have:
- Python 3.11 or higher
- A graph database (Neo4j or ArangoDB) if you plan to use database features
- Basic understanding of Python and graph databases

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
uv sync --dev
```

## Optional Dependencies

GraphCast has some optional dependencies that can be installed based on your needs:

```bash
# For graph visualization
pip install graphcast[graphviz]
```

## Verifying Installation

To verify your installation, you can run:

```python
import graphcast
print(graphcast.__version__)
```

## Configuration

After installation, you may need to configure your graph database connection. See the [Quick Start Guide](quickstart.md) for details on setting up your environment.

## Troubleshooting

If you encounter any issues during installation:

1. Ensure you have the correct Python version (3.11+)
2. Check if all required system dependencies are installed
3. Verify your virtual environment is properly activated
4. Check the error messages for specific package conflicts

For more detailed troubleshooting, refer to the [API Reference](reference/index.md) or open an issue on GitHub. 