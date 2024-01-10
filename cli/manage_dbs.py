import logging
import os
import subprocess
import sys
from datetime import date

import click
from suthing import ArangoConnectionConfig, ConfigFactory, FileHandle, Timer

logger = logging.getLogger(__name__)


def act_db(
    conf: ArangoConnectionConfig,
    db_name,
    output_path,
    restore,
    docker_version,
    use_docker,
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
    host = f"tcp://{conf.ip_addr}:{conf.port}"
    db_folder = os.path.join(output_path, db_name)

    cmd = "arangorestore" if restore else "arangodump"
    if use_docker:
        ru = (
            f"docker run -it --rm -v {db_folder}:/dump"
            f" arangodb/arangodb:{docker_version} {cmd}"
        )
        output = "--output-directory /dump"
    else:
        ru = f"{cmd}"
        output = f"--output-directory {db_folder}"

    dir_spec = "input" if restore else "output"

    query = f"""
        {ru} --server.endpoint {host} --server.username {conf.cred_name} --server.password "{conf.cred_pass}" --{dir_spec}-directory {output} --server.database "{db_name}" """

    restore_suffix = "--create-database true --force-same-database true"
    if restore:
        query += restore_suffix

    subprocess.run(query, shell=True)


@click.command()
@click.option("--db-config-path", type=click.Path())
@click.option("--db", type=str, multiple=True)
@click.option("--store-directory-path", type=click.Path())
@click.option("--docker-version", type=str, default="3.10.6")
@click.option("--restore", type=bool, default=False)
@click.option("--use-docker", type=bool, default=True)
def manage_dbs(
    db_config_path,
    db,
    store_directory_path,
    docker_version,
    restore=False,
    use_docker=True,
):
    """
    dump/restore arango databases
    """
    conn_conf = FileHandle.load(fpath=db_config_path)
    db_conf: ArangoConnectionConfig = ConfigFactory.create_config(
        dict_like=conn_conf
    )

    action = "restoring" if restore else "dumping"
    if restore:
        out_path = store_directory_path
    else:
        out_path = os.path.join(store_directory_path, date.today().isoformat())

    exists = os.path.exists(out_path)
    if not exists:
        os.makedirs(out_path)

    with Timer() as t_all:
        for db in db:
            with Timer() as t_dump:
                try:
                    act_db(
                        db_conf,
                        db,
                        out_path,
                        restore=restore,
                        docker_version=docker_version,
                        use_docker=use_docker,
                    )
                except Exception as e:
                    print(e)
            print(
                f"{action} {db} took  {t_dump.mins} mins {t_dump.secs:.2f} sec"
            )
    print(f"all {action} took  {t_all.mins} mins {t_all.secs:.2f} sec")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    manage_dbs()
