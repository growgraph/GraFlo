# Examples

This page provides practical examples of using GraphCast for different scenarios.

## Basic Data Transformation

### CSV to Graph

```csv
# people.csv
id,name,age
1,John Dow,27
2,Mary Arpe,33
```

```yaml
# company.yml
general:
    name: company
resources:
    row_likes:
    -   name: people
    -   name:
vertex_config:
    vertices:
    -   name: person
        fields:
        -   id
        -   name
        -   age
        indexes:
        -   fields:
            -   id
    -   name: company
        fields:
        -   id
        -   name
        -   industry
        indexes:
        -   fields:
            -   id
edge_config:
    edges:
    -   source: person
        target: company
```


```python
from pathlib import Path
from graphcast import Caster, Schema

schema = Schema("company.yaml")

# Initialize caster and process data
caster = Caster(schema)
caster.ingest_files(Path("data/"))
```

### JSON to Graph

```python
from graphcast import Caster, Schema

# Define schema for JSON data
schema = Schema({
    "vertices": {
        "User": {
            "source": "users.json",
            "properties": ["id", "username", "email"]
        },
        "Post": {
            "source": "posts.json",
            "properties": ["id", "title", "content"]
        }
    },
    "edges": {
        "CREATED": {
            "source": "posts.json",
            "from": "User",
            "to": "Post",
            "properties": ["created_at"]
        }
    }
})

# Process JSON data
caster = Caster(schema)
caster.ingest_files(Path("data/"))
```

## Advanced Use Cases

### Parallel Processing

```python
# Process large datasets efficiently
caster = Caster(
    schema,
    n_cores=4,      # Use 4 CPU cores
    n_threads=8,    # 8 threads per core
    batch_size=10000  # Process 10,000 items at a time
)
```

### Database Integration

```python
from graphcast import ConnectionManager, ConnectionType

# Configure Neo4j connection
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

### Data Filtering

```python
from graphcast import ComparisonOperator, LogicalOperator

# Define filters for data transformation
schema = Schema({
    "vertices": {
        "Product": {
            "source": "products.csv",
            "properties": ["id", "name", "price"],
            "filters": [
                {
                    "property": "price",
                    "operator": ComparisonOperator.GREATER_THAN,
                    "value": 100
                }
            ]
        }
    }
})
```

## Real-World Scenarios

### Knowledge Graph Construction

```python
# Building a knowledge graph from research papers
schema = Schema({
    "vertices": {
        "Paper": {
            "source": "papers.json",
            "properties": ["doi", "title", "abstract"]
        },
        "Author": {
            "source": "authors.json",
            "properties": ["id", "name", "affiliation"]
        },
        "Keyword": {
            "source": "keywords.json",
            "properties": ["term", "category"]
        }
    },
    "edges": {
        "AUTHORED": {
            "source": "papers.json",
            "from": "Author",
            "to": "Paper",
            "properties": ["order"]
        },
        "HAS_KEYWORD": {
            "source": "papers.json",
            "from": "Paper",
            "to": "Keyword",
            "properties": ["relevance"]
        }
    }
})
```

### Social Network Analysis

```python
# Transforming social media data into a graph
schema = Schema({
    "vertices": {
        "User": {
            "source": "users.json",
            "properties": ["id", "username", "join_date"]
        },
        "Post": {
            "source": "posts.json",
            "properties": ["id", "content", "timestamp"]
        }
    },
    "edges": {
        "FOLLOWS": {
            "source": "relationships.json",
            "from": "User",
            "to": "User",
            "properties": ["since"]
        },
        "LIKES": {
            "source": "interactions.json",
            "from": "User",
            "to": "Post",
            "properties": ["timestamp"]
        }
    }
})
```

## Best Practices

1. **Schema Design**
   - Keep vertex and edge types meaningful and consistent
   - Use appropriate property types for your data
   - Consider indexing frequently queried properties

2. **Performance Optimization**
   - Use batch processing for large datasets
   - Enable parallel processing when possible
   - Monitor memory usage during transformation

3. **Data Quality**
   - Implement data validation in your schema
   - Use filters to clean and transform data
   - Handle missing or invalid data appropriately

For more examples and detailed explanations, refer to the [API Reference](../reference/index.md). 