# Concepts

Here we introduce the main concepts of GraphCast.

## Basic Concepts

- `Schema` encapsulates a set of rules required to cast data sources to graph databases.
- `Vertex` - describes vertices and their database indexes. 
- `Edge` - describes edges their database indexes.
- `Resource` - a set of mappings and transformations of a data source to vertices and edges, defined a hierarchical structure (basically a tree of `Actors`)
- `Actor` - a vertex describing how the current level of the document should be mapped/transformed to the property graph vertices and edges
- `Transform` - defines data transforms, from renaming and type-casting, to arbitrary transforms defined as python functions

## Features
1. **Schema Design**

- Vertices may be indexed by multiple fields, e.g. `["first_name", "last_name]` instead of `"full_name"`.
- Edges can be defined at any level of a hierarchical document and rely on vertex principal index
- Edges can pick up weights from the document or vertices using `source_fields`, `target_fields` and `direct` parameters of `WeightConfig`
- Blank vertices may be defined for convenience
- `Resource` at each level may contain 4 types of Actors: `DescendActor`, `TransformActor`, `VertexActor` and `EdgeActor` which act on the provided document in this order.
- There is no need to specify whether the next level contains a document or a list of document, `DescendActor` automatically treats both cases
- It is possible to constrain edges to be unique with respect to `source`, `target`, `weight` fields
- `Transforms` may be provided in the `transforms` section of `Schema` and referenced by their `name`
- `Vertex` instances can be filtered via `filters` field in vertex definition

2. **Performance Optimization**
   - Use batch processing for large datasets (`batch_size` parameter of `Caster`)
   - Enable parallel processing when possible (`n_cores` parameter of `Caster`)

