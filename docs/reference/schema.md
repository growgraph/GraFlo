# Schema and Resources

This guide explains how to define and work with schemas and resources in GraphCast.

## Schema Structure

A schema in GraphCast defines how your data should be transformed into a graph. It consists of several key components:

```python
from graphcast import Schema

schema = Schema({
    "general": {
        "name": "my_graph"
    },
    "vertex_config": {
        "vertices": [
            {
                "name": "Person",
                "fields": ["id", "name", "age"],
                "indexes": [
                    {
                        "fields": ["id"],
                        "unique": True
                    }
                ]
            }
        ],
        "blank_vertices": ["IntermediateNode"]
    },
    "edge_config": {
        "edges": [
            {
                "source": "Person",
                "target": "Person",
                "type": "DIRECT",
                "indexes": [
                    {
                        "fields": ["_from", "_to"],
                        "unique": True
                    }
                ]
            }
        ]
    },
    "resources": {
        "row_likes": [
            {
                "name": "people",
                "source": "people.csv",
                "type": "CSV"
            }
        ]
    }
})
```

## Vertex Definitions

Vertices are defined with:
- **name**: The vertex type name
- **fields**: List of property names
- **indexes**: Unique identifiers for vertices
- **filters**: Conditions for vertex creation
- **transforms**: Data transformations

### Unique Vertices

Vertices can be made unique using indexes:

```python
{
    "name": "Person",
    "fields": ["id", "name", "age"],
    "indexes": [
        {
            "fields": ["id"],
            "unique": True
        }
    ]
}
```

## Edge Definitions

Edges define relationships between vertices:
- **source**: Source vertex type
- **target**: Target vertex type
- **type**: Edge type (DIRECT or INDIRECT)
- **indexes**: Unique identifiers for edges
- **properties**: Edge properties

### Edge Types

1. **Direct Edges**: Direct relationships between vertices
2. **Indirect Edges**: Relationships through intermediate vertices

## Resources

Resources are your data sources:

### Table-like Resources

```python
{
    "name": "people",
    "source": "people.csv",
    "type": "CSV",
    "mapping": {
        "id": "person_id",
        "name": "full_name"
    }
}
```

### JSON-like Resources

```python
{
    "name": "users",
    "source": "users.json",
    "type": "JSON",
    "path": "$.users[*]"
}
```

## Filters

Filters allow you to control which data gets transformed:

```python
{
    "vertices": {
        "Product": {
            "filters": [
                {
                    "property": "price",
                    "operator": "GREATER_THAN",
                    "value": 100
                }
            ]
        }
    }
}
```

## Blank Nodes

Blank nodes are intermediate vertices used in complex relationships:

```python
{
    "vertex_config": {
        "blank_vertices": ["IntermediateNode"],
        "vertices": [
            {
                "name": "IntermediateNode",
                "fields": ["id", "type"]
            }
        ]
    }
}
```

## Best Practices

1. **Schema Design**
   - Use meaningful names for vertices and edges
   - Define appropriate indexes for efficient querying
   - Keep property names consistent across related vertices

2. **Resource Organization**
   - Group related data in the same resource
   - Use appropriate data types for properties
   - Consider data volume when choosing resource types

3. **Performance Optimization**
   - Use indexes for frequently queried properties
   - Minimize the number of blank nodes
   - Optimize edge definitions for your use case

For more examples and detailed explanations, refer to the [Examples](examples.md) section. 