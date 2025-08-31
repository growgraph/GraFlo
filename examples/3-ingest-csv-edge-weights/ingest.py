from suthing import ConfigFactory, FileHandle

from graflo import Caster, Patterns, Schema

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
            "people": {"regex": "^relations.*\.csv$"},
        }
    }
)

caster = Caster(schema)

caster.ingest_files(path=".", conn_conf=conn_conf, patterns=patterns, clean_start=True)
