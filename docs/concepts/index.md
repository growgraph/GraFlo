# Concepts

Here we introduce the main concepts of GraphCast, a framework for transforming data into property graphs.

## Core Components

### Schema
The `Schema` encapsulates a set of rules required to cast data sources to graph databases. It defines:
- Vertex and edge definitions
- Resource mappings
- Data transformations
- Index configurations

### Vertex
A `Vertex` describes vertices and their database indexes. It supports:
- Single or compound indexes (e.g., `["first_name", "last_name"]` instead of `"full_name"`)
- Property definitions
- Filtering conditions
- Optional blank vertex configuration

### Edge
An `Edge` describes edges and their database indexes. It allows:
- Definition at any level of a hierarchical document
- Reliance on vertex principal index
- Weight configuration using `source_fields`, `target_fields`, and `direct` parameters
- Uniqueness constraints with respect to `source`, `target`, and `weight` fields

### Resource
A `Resource` is a set of mappings and transformations of a data source to vertices and edges, defined as a hierarchical structure (basically a tree of `Actors`). It supports:
- Table-like data (CSV, SQL)
- Tree-like data (JSON, XML)
- Complex nested structures

### Actor
An `Actor` describes how the current level of the document should be mapped/transformed to the property graph vertices and edges. There are four types that act on the provided document in this order:
- `DescendActor`: Navigates to the next level in the hierarchy
- `TransformActor`: Applies data transformations
- `VertexActor`: Creates vertices from the current level
- `EdgeActor`: Creates edges between vertices

### Transform
A `Transform` defines data transforms, from renaming and type-casting to arbitrary transforms defined as Python functions. Transforms can be:
- Provided in the `transforms` section of `Schema`
- Referenced by their `name`
- Applied to both vertices and edges

## Key Features

### Schema Design
- **Flexible Indexing**: Support for compound indexes on vertices and edges
- **Hierarchical Edge Definition**: Define edges at any level of nested documents
- **Weighted Edges**: Configure edge weights from document fields or vertex properties
- **Blank Vertices**: Create intermediate vertices for complex relationships
- **Actor Pipeline**: Process documents through a sequence of specialized actors
- **Smart Navigation**: Automatic handling of both single documents and lists
- **Edge Constraints**: Ensure edge uniqueness based on source, target, and weight
- **Reusable Transforms**: Define and reference transformations by name
- **Vertex Filtering**: Filter vertices based on custom conditions

### Performance Optimization
- **Batch Processing**: Process large datasets in configurable batches (`batch_size` parameter of `Caster`)
- **Parallel Execution**: Utilize multiple cores for faster processing (`n_cores` parameter of `Caster`)
- **Efficient Resource Handling**: Optimized processing of both table and tree-like data
- **Smart Caching**: Minimize redundant operations

## Best Practices
1. Use compound indexes for frequently queried vertex properties
2. Leverage blank vertices for complex relationship modeling
3. Define transforms at the schema level for reusability
4. Configure appropriate batch sizes based on your data volume
5. Enable parallel processing for large datasets

