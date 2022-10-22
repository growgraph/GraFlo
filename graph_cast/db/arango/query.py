import gzip
import json
import logging
from os.path import join

from arango import ArangoClient

logger = logging.getLogger(__name__)


def basic_query(
    query,
    port=8529,
    ip_addr="127.0.0.1",
    cred_name="root",
    cred_pass="123",
    db_name="_system",
    profile=False,
    batch_size=10000,
    bind_vars=None,
):
    hosts = f"http://{ip_addr}:{port}"
    client = ArangoClient(hosts=hosts)

    sys_db = client.db(db_name, username=cred_name, password=cred_pass)
    cursor = sys_db.aql.execute(
        query,
        profile=profile,
        stream=True,
        batch_size=batch_size,
        bind_vars=bind_vars,
    )
    return cursor


def profile_query(query, nq, profile_times, fpath, limit=None, **kwargs):
    limit_str = f"_limit_{limit}" if limit else ""
    if profile_times:
        logger.info(f"starting profiling: {limit}")
        profiling = []
        for n in range(profile_times):
            cursor = basic_query(query, profile=True, **kwargs)
            profiling += [cursor.profile()]
            cursor.close()
        with open(
            join(fpath, f"query{nq}_profile{limit_str}.json"), "w"
        ) as fp:
            json.dump(profiling, fp, indent=4)

    logger.info(f"starting actual query at {limit}")

    cnt = 0
    cursor = basic_query(query, **kwargs)
    chunk = list(cursor.batch())
    with gzip.open(
        join(fpath, f"./query{nq}_result{limit_str}_batch_{cnt}.json.gz"),
        "wt",
        encoding="ascii",
    ) as fp:
        json.dump(chunk, fp, indent=4)

    while cursor.has_more():
        cnt += 1
        with gzip.open(
            join(fpath, f"./query{nq}_result{limit_str}_batch_{cnt}.json.gz"),
            "wt",
            encoding="ascii",
        ) as fp:
            chunk = list(cursor.fetch()["batch"])
            json.dump(chunk, fp, indent=4)
