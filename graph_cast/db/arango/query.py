import gzip
import json
import logging
from os.path import join

from arango import ArangoClient

from graph_cast.filter.onto import Expression
from graph_cast.onto import DBFlavor

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


def fetch_fields_query(
    collection_name,
    docs,
    match_keys,
    return_keys,
    filters: list | dict | None = None,
):
    """

    :param collection_name: collection to look up docs
    :param docs: list of dicts (json-like, ie keys are strings)
    :param match_keys: keys on which to look for document
    :param return_keys: keys which to return
    :param filters:
    :return:
    """

    docs_ = [{k: doc[k] for k in match_keys if k in doc} for doc in docs]
    for i, doc in enumerate(docs_):
        doc.update({"__i": i})

    docs_str = json.dumps(docs_)

    match_str = " &&".join(
        [f" _cdoc['{key}'] == _doc['{key}']" for key in match_keys]
    )

    return_vars = [x.replace("@", "_") for x in return_keys]

    keep_clause = (
        f"KEEP(_x, {list(return_vars)})" if return_vars is not None else "_x"
    )

    if filters is not None:
        ff = Expression.from_dict(filters)
        extrac_filter_clause = (
            f" && {ff(doc_name='_cdoc', kind=DBFlavor.ARANGO)}"
        )
    else:
        extrac_filter_clause = ""

    q0 = f"""
        FOR _cdoc in {collection_name}
            FOR _doc in {docs_str}
                FILTER {match_str} {extrac_filter_clause}      
                COLLECT i = _doc['__i'] into _group = _cdoc 
                LET gp = (for _x in _group return {keep_clause})                                
                    RETURN {{'__i' : i, '_group': gp}}"""
    return q0
