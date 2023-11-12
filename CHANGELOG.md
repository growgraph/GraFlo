# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.12.0] - 2023-10

### Added

- `cli/plot_schema.py` became a standalone script available with the package installation
-  basic `neo4j` ingestion added:
     - create_database
     - delete_database
     - define_vertex_indices
     - define_edge_indices
     - delete_collections
     - init_db
     - upsert_docs_batch
     - insert_edges_batch

### Fixed

- ***

### Changed

- in `ingest_json_files`: ncores -> n_threads 
- schema config changes:
    - `type` specification removed in Transform (field mapping) specification, whenever ambiguous, `image` is used   


## [0.11.5] - 2023-08-30

### Fixed

- not more info level logging, only debug

### Changed

- in `ingest_json_files`: ncores -> n_threads
- in `ingest_tables`: n_thread -> n_threads
- added a single entry point for file ingestion : `ingest_files`
- added docker-compose config for Arango; all tests talk to it automatically
- `init_db` now is member of `Connection`
- Introduced `InputType` as `Enum` : {`TABLE`, `JSON`}


## [0.11.3] - 2023-06-24

### Fixed

- suthing version

### Changed

- dev dependency were moved to `dev` group, graphviz was moved to extra group

## [0.11.2] - 2023-06-20

### Fixed

- schema plotting for tables and jsons

### Changed

- introduced `DataSourceType` as `Enum` instead of `str`

## [0.11.1] - 2023-06-14

### Added

- versions via tags
- changelog.MD

[//]: # (### Changed)

[//]: # ()
[//]: # (### Fixed)






