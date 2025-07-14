from suthing import ConfigFactory, FileHandle

from graphcast import Caster, Patterns, Schema

schema = Schema.from_dict(FileHandle.load("schema.yaml"))

conn_conf = ConfigFactory.create_config(
    {
        "protocol": "bolt",
        "hostname": "localhost",
        "port": 7688,
        "username": "neo4j",
        "password": "test!passfortesting",
    }
)

patterns = Patterns.from_dict(
    {
        "patterns": {
            "package": {"regex": r"^package.meta.*\.json$"},
            # "bugs": {"regex": r"^bugs.head.*\.json$"},
        }
    }
)

caster = Caster(schema)

caster.ingest_files(
    path="~/data/deb-kg/tmp", conn_conf=conn_conf, patterns=patterns, max_items=5
)
