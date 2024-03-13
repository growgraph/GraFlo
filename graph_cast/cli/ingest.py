import logging.config
import pathlib
from os.path import dirname, join, realpath

import click
from suthing import ConfigFactory, FileHandle

from graph_cast.architecture.schema import Schema
from graph_cast.caster import Caster
from graph_cast.util.onto import Patterns

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--db-config-path",
    type=click.Path(exists=True, path_type=pathlib.Path),
    required=True,
)
@click.option(
    "--schema-path",
    type=click.Path(exists=True, path_type=pathlib.Path),
    required=True,
)
@click.option(
    "--source-path",
    type=click.Path(exists=True, path_type=pathlib.Path),
    required=True,
)
@click.option(
    "--resource-pattern-config-path",
    type=click.Path(exists=True, path_type=pathlib.Path),
    default=None,
)
@click.option("--limit-files", type=int, default=None)
@click.option("--batch-size", type=int, default=5000)
@click.option("--n-cores", type=int, default=1)
@click.option(
    "--n-threads",
    type=int,
    default=1,
)
@click.option("--fresh-start", type=bool, help="wipe existing database")
def ingest(
    db_config_path,
    schema_path,
    source_path,
    limit_files,
    batch_size,
    n_cores,
    n_threads,
    fresh_start,
    resource_pattern_config_path,
):
    cdir = dirname(realpath(__file__))

    logging.config.fileConfig(
        join(cdir, "../logging.conf"), disable_existing_loggers=False
    )

    logging.basicConfig(level=logging.INFO)

    schema = Schema.from_dict(FileHandle.load(fpath=schema_path))

    conn_conf = ConfigFactory.create_config(
        dict_like=FileHandle.load(fpath=db_config_path)
    )

    if resource_pattern_config_path is not None:
        patterns = Patterns.from_dict(
            FileHandle.load(fpath=resource_pattern_config_path)
        )
    else:
        patterns = Patterns()

    schema.fetch_resource()

    caster = Caster(
        schema,
        n_cores=n_cores,
        n_threads=n_threads,
    )

    caster.ingest_files(
        path=source_path,
        limit_files=limit_files,
        clean_start=fresh_start,
        batch_size=batch_size,
        conn_conf=conn_conf,
        patterns=patterns,
    )


if __name__ == "__main__":
    ingest()
