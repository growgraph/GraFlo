from pathlib import Path

from suthing import ConfigFactory, FileHandle

from graphcast import Caster, Patterns, Schema

schema = Schema.from_dict(FileHandle.load("schema.yaml"))

caster = Caster(schema)

conn_conf = ConfigFactory.create_config(
    {
        "protocol": "http",
        "hostname": "localhost",
        "port": 8535,
        "cred_name": "root",
        "cred_pass": "123",
        "database": "_system",
    }
)

patterns = Patterns.from_dict(
    {
        "patterns": {
            "people": {"regex": "^people.*\.csv$"},
            "departments": {"regex": "^dep.*\.csv$"},
        }
    }
)

caster.ingest_files(
    path=Path("."),
    conn_conf=conn_conf,
    patterns=patterns,
)
