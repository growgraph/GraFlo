import logging
import pathlib
import subprocess
import sys
from datetime import date

import click
from suthing import ArangoConnectionConfig, ConfigFactory, FileHandle, Timer

logger = logging.getLogger(__name__)


def act_db(
    conf: ArangoConnectionConfig,
    db_name: str,
    output_path: pathlib.Path,
    restore: bool,
    docker_version: str,
    use_docker: bool,
):
    """

    :param conf: conf to use for connecting
    :param db_name: db to dump
    :param output_path: eg /root/dumps/arango/
    :param restore: restore
    :param docker_version:
    :param use_docker:
    :return:
    """
    host = f"tcp://{conf.hostname}:{conf.port}"
    db_folder = output_path / db_name

    cmd = "arangorestore" if restore else "arangodump"
    if use_docker:
        ru = (
            f"docker run --rm --network=host -v {db_folder}:/dump"
            f" arangodb/arangodb:{docker_version} {cmd}"
        )
        output = "--output-directory /dump"
    else:
        ru = f"{cmd}"
        output = f"--output-directory {db_folder}"

    dir_spec = "input" if restore else "output"

    query = f"""{ru} --server.endpoint {host} --server.username {conf.cred_name} --server.password "{conf.cred_pass}" --{dir_spec}-directory {output} --server.database "{db_name}" """

    restore_suffix = "--create-database true --force-same-database true"
    if restore:
        query += restore_suffix
    else:
        query += "--overwrite true"

    flag = subprocess.run(query, shell=True)
    logger.info(f"returned {flag}")


@click.command()
@click.option(
    "--db-config-path",
    type=click.Path(exists=True, path_type=pathlib.Path),
    required=False,
    default=None,
)
@click.option("--db-host", type=str)
@click.option("--db-password", type=str)
@click.option("--db-user", type=str, default="root")
@click.option(
    "--db",
    type=str,
    multiple=True,
    required=True,
    help="filesystem path where to dump db snapshot",
)
@click.option(
    "--store-directory-path",
    type=click.Path(path_type=pathlib.Path),
    required=True,
    help="filesystem path where to dump db snapshot",
)
@click.option("--docker-version", type=str, default="3.12.1")
@click.option("--restore", type=bool, default=False, is_flag=True)
@click.option("--use-docker", type=bool, default=True)
def manage_dbs(
    db_config_path,
    db_host,
    db_password,
    db_user,
    db,
    store_directory_path,
    restore,
    docker_version,
    use_docker=True,
):
    """
    dump/restore arango databases
    either arangosh or docker should be available in the system
    """

    if db_config_path is None:
        db_conf: ArangoConnectionConfig = ArangoConnectionConfig(
            cred_name=db_user, cred_pass=db_password, hosts=db_host
        )
    else:
        conn_conf = FileHandle.load(fpath=db_config_path)
        db_conf: ArangoConnectionConfig = ConfigFactory.create_config(
            dict_like=conn_conf
        )

    action = "restoring" if restore else "dumping"
    if restore:
        out_path = store_directory_path
    else:
        out_path = (
            store_directory_path.expanduser().resolve() / date.today().isoformat()
        )

        if not out_path.exists():
            out_path.mkdir(exist_ok=True)

    with Timer() as t_all:
        for dbname in db:
            with Timer() as t_dump:
                try:
                    act_db(
                        db_conf,
                        dbname,
                        out_path,
                        restore=restore,
                        docker_version=docker_version,
                        use_docker=use_docker,
                    )
                except Exception as e:
                    logging.error(e)
            logging.info(
                f"{action} {dbname} took  {t_dump.mins} mins {t_dump.secs:.2f} sec"
            )
    logging.info(f"all {action} took  {t_all.mins} mins {t_all.secs:.2f} sec")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    manage_dbs()
