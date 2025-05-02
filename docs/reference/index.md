# API Reference

This section provides detailed documentation for the GraphCast API.

## Core Components

### Caster
The main class responsible for transforming data into graph structures.

```python
from graphcast import Caster

# Initialize with schema
caster = Caster(schema)
```

Key methods:
- `ingest_files()`: Process files from a directory
- `process_resource()`: Process a single data resource
- `push_db()`: Store graph data in a database

### Schema
Defines the structure of your graph transformation.

```python
from graphcast import Schema

schema = Schema({
    "vertices": {...},
    "edges": {...}
})
```

### ConnectionManager
Manages database connections for graph storage.

```python
from graphcast import ConnectionManager, ConnectionType

conn = ConnectionManager(
    connection_type=ConnectionType.NEO4J,
    host="localhost",
    port=7687
)
```

## Data Types

### DataSourceType
Enum defining supported data source types:
- CSV
- JSON
- XML

### ConnectionType
Enum defining supported database types:
- NEO4J
- ARANGO

## Utility Classes

### Patterns
Utility class for working with file patterns and data matching.

### ChunkerFactory
Factory for creating data chunkers for batch processing.

## Filtering and Transformation

### ComparisonOperator
Operators for data comparison:
- EQUALS
- NOT_EQUALS
- GREATER_THAN
- LESS_THAN
- etc.

### LogicalOperator
Logical operators for combining filters:
- AND
- OR
- NOT

## Detailed Documentation

For more detailed information about each component, please refer to the specific documentation pages:

- [Core Module](core.md)
- [Connection Module](connection.md)
- [Schema Definition](schema.md)
- [Filtering and Transformation](filters.md)
- [Utility Functions](utils.md) 