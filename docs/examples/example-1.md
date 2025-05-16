# Example 1: multiple types of csv files


## Basic Data Transformation

Suppose you have a table that represent people:

```csv
# people.csv
id,name,age
1,John Hancock,27
2,Mary Arpe,33
3,Sid Mei,45
```

and a table that represents their roles in a company:

```csv
# departments
person_id,person,department
1,John Dow,Sales
2,Mary Arpe,R&D
3,Sid Mei,Customer Service
```

```yaml
# schema.yaml
general:
    name: hr
resources:
-   resource_name: people
    apply:
    -   vertex: person
-   resource_name: departments
    apply:
    -   map:
            person: name
            person_id: id
    -   target_vertex: department
        map:
            department: name
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
    -   name: department
        fields:
        -   name
        indexes:
        -   fields:
            -   name
edge_config:
    edges:
    -   source: person
        target: department
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