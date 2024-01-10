import logging.config

import click
from suthing import ConfigFactory, FileHandle

from graph_cast.architecture.schema import Schema
from graph_cast.caster import Caster
from graph_cast.util.onto import Patterns

logger = logging.getLogger(__name__)


@click.command()
@click.option("--db-config-path", type=click.Path())
@click.option("--schema-path", type=click.Path())
@click.option("--source-path", type=click.Path())
@click.option("--limit-files", type=int | None, default=None)
@click.option("-b", "--batch-size", default=5000, type=int)
@click.option("--n-cores", default=1, type=int)
@click.option("--n-threads", default=1, type=int)
@click.option("--fresh-start", type=bool, help="wipe existing database")
@click.option("--resource-pattern-config-path", type=click.Path())
def ingest(
    schema_path,
    db_config_path,
    source_path,
    limit_files,
    batch_size,
    n_cores,
    n_threads,
    fresh_start,
    resource_pattern_config_path,
):
    name = schema_path.split(".")[-2]
    logging.basicConfig(
        format=(
            "%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s:"
            " %(message)s"
        ),
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
        filemode="w",
    )
    logging.config.fileConfig(
        f"ingest_{name}.log", disable_existing_loggers=False
    )

    schema = Schema.from_dict(FileHandle.load(fpath=schema_path))

    patterns = Patterns.from_dict(
        FileHandle.load(fpath=resource_pattern_config_path)
    )

    conn_conf = ConfigFactory.create_config(
        dict_like=FileHandle.load(fpath=db_config_path)
    )

    schema.fetch_resource()

    caster = Caster(schema)
    caster.ingest_files(
        path=source_path,
        limit_files=limit_files,
        clean_start=fresh_start,
        batch_size=batch_size,
        conn_conf=conn_conf,
        n_cores=n_cores,
        n_threads=n_threads,
        patterns=patterns,
    )


if __name__ == "__main__":
    ingest()
