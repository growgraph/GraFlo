# Quick Start Guide

This guide will help you get started with GraphCast by showing you how to transform data into a graph structure.

## Basic Concepts

GraphCast works with three main components:
1. **Data Sources**: Your input data (CSV, JSON, XML)
2. **Schema**: Defines how to transform the data into vertices and edges
3. **Caster**: The main class that performs the transformation

## Basic Example

Here's a simple example of transforming a CSV file into a graph:

```python
from pathlib import Path
from graphcast import Caster, Schema, ConnectionManager, ConnectionType

# Define your schema
schema = Schema({
    "vertices": {
        "Person": {
            "source": "people.csv",
            "properties": ["name", "age"]
        }
    },
    "edges": {
        "KNOWS": {
            "source": "relationships.csv",
            "from": "Person",
            "to": "Person",
            "properties": ["since"]
        }
    }
})

# Initialize the caster
caster = Caster(schema)

# Process your data
caster.ingest_files(Path("data/"))
```

## Working with Different Data Sources

### CSV Files

```python
# Example with CSV data
schema = Schema({
    "vertices": {
        "Product": {
            "source": "products.csv",
            "properties": ["id", "name", "price"]
        }
    }
})
```

### JSON Data

```python
# Example with JSON data
schema = Schema({
    "vertices": {
        "User": {
            "source": "users.json",
            "properties": ["id", "username", "email"]
        }
    }
})
```

## Database Integration

To store your graph in a database:

```python
# Configure database connection
conn_config = ConnectionManager(
    connection_type=ConnectionType.NEO4J,
    host="localhost",
    port=7687,
    username="neo4j",
    password="password"
)

# Process and store data
caster.ingest_files(Path("data/"), conn_conf=conn_config)
```

## Advanced Features

### Parallel Processing

```python
# Enable parallel processing
caster = Caster(
    schema,
    n_cores=4,  # Number of CPU cores to use
    n_threads=8  # Number of threads per core
)
```

### Batch Processing

```python
# Configure batch size
caster = Caster(
    schema,
    batch_size=10000  # Process 10,000 items at a time
)
```

## Next Steps

- Explore the [API Reference](reference/index.md) for detailed documentation
- Check out more [Examples](examples.md) for advanced use cases
- Learn about [Schema Definition](reference/schema.md) for complex transformations 