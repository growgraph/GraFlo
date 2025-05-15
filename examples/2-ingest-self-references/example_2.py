from pathlib import Path

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
    path=Path("."), conn_conf=conn_conf, patterns=patterns, clean_start=True
)
