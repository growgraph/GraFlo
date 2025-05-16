# Example 1: Multiple Tabular Sources

Openalex is a comprehensive academic dataset. One of categories of their API entities is [Works](https://docs.openalex.org/api-entities/works). 
Suppose we want to create a graph that contains a description of academic works and correctly map the references to works that either already exist in the database or will be populated later:

```json
{
    "id": "https://openalex.org/W4300681084",
    "doi": "https://doi.org/10.15460/hup.78",
    "publication_date": "2006-01-23",
    "referenced_works": [
        "https://openalex.org/W617126653",
        "https://openalex.org/W658636438",
        "https://openalex.org/W2010066730",
        "https://openalex.org/W2039589765",
        "https://openalex.org/W2344506138"
    ],
}
```

In this example we will be interested in how to create vertices `Work` and `Work` &rarr; `Work` edges.

Let's define vertices as

```yaml
 vertices:
 -   name: work
     fields:
     -   _key
     -   doi
     indexes:
     -   fields:
         -   _key
     -   unique: false
         fields:
         -   doi
```

The graph structure is quite simple:

![People Resource Image](../assets/2-ingest-self-references/figs/openalex_vc2vc.png){ width="200" }

We will be using a transformation that truncates the suffix from a url, e.g. "https://openalex.org/W4300681084" &rarr; "W4300681084". So let's define a section for transforms that will contain reusable transforms, specified by `name`:

```yaml
transforms:
    keep_suffix_id:
        foo: split_keep_part
        module: graphcast.util.transform
        params:
            sep: "/"
            keep: -1
        input:
        -   id
        output:
        -   _key
```

Let's define the mappings. We will apply `keep_suffix_id` to `id` and `doi` fields in several places with slightly different parameters. We will map the corresponding values to define instances of `work` vertex and define edges `work` &rarr; `work`. 

```yaml
-   resource_name: work
    apply:
    -   name: keep_suffix_id
    -   name: keep_suffix_id
        params:
            sep: "/"
            keep: [-2, -1]
        input:
        -   doi
        output:
        -   doi
    -   vertex: work
        discriminant: _top_level
    -   key: referenced_works
        apply:
        -   vertex: work
        -   name: keep_suffix_id
    -   source: work
        target: work
        source_discriminant: _top_level
```

Works Resource

![Department Resource Image](../assets/2-ingest-self-references/figs/openalex.resource-work.png){ width="800" }


Now as you noticed the document has a nested list under `referenced_works`. In order to deal with which we introduce a `DescendActor`, that contains a transform and a mapping to `Work` vertex under `apply`.

Since we are defining a `refers to` relation, we need some extra configuration:
- we temporarily split works into two groups by add a discriminant to the top level `Work` vertex:
    ```yaml
    vertex: work
    discriminant: _top_level
    ```
- the edge is then defined by picking `_top_level` vertices as specified by `source_discriminant` attribute:
    ```yaml
    source: work
    target: work
    source_discriminant: _top_level
    ```
Transforming the data and ingesting it into an ArangoDB takes a few lines of code:

```python
from suthing import ConfigFactory, FileHandle
from graphcast import Caster, Patterns, Schema

schema = Schema.from_dict(FileHandle.load("schema.yaml"))

caster = Caster(schema)

conn_conf = ConfigFactory.create_config(FileHandle.load("../arango.creds.json"))

patterns = Patterns.from_dict(
    {
        "patterns": {
            "work": {"regex": "\Sjson$"},
        }
    }
)

caster.ingest_files(
    path=".", conn_conf=conn_conf, patterns=patterns, clean_start=True
)
```

Please refer to [examples](https://github.com/growgraph/graphcast/tree/main/examples/1-ingest-csv)

For more examples and detailed explanations, refer to the [API Reference](../reference/index.md). 