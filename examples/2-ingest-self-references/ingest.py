from suthing import ConfigFactory, FileHandle

from graphcast import Caster, Patterns, Schema

schema = Schema.from_dict(FileHandle.load("schema.yaml"))

caster = Caster(schema)

conn_conf = ConfigFactory.create_config(
    {
        "protocol": "http",
        "hostname": "localhost",
        "port": 8535,
        "username": "root",
        "password": "123",
        "database": "_system",
    }
)

patterns = Patterns.from_dict(
    {
        "patterns": {
            "work": {"regex": "\Sjson$"},
        }
    }
)

caster.ingest_files(path=".", conn_conf=conn_conf, patterns=patterns, clean_start=True)
