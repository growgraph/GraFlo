import json
import logging
from collections import defaultdict

from graph_cast.architecture.schema import Edge

logger = logging.getLogger(__name__)


def define_extra_edges(g: Edge):
    """
    create a query to generate edges from u to v by w :
            (u -> w -> v) -> (u -> w) and add properties of w as properties of the edge


    :param g:
    :return:
    """
    ucol, vcol, wcol = g.source, g.target, g.by
    weight = g.weight_dict
    s = f"""FOR w IN {wcol}
        LET uset = (FOR u IN 1..1 INBOUND w {ucol}_{wcol}_edges RETURN u)
        LET vset = (FOR v IN 1..1 INBOUND w {vcol}_{wcol}_edges RETURN v)
        FOR u in uset
        FOR v in vset
    """
    s_ins_ = ", ".join([f"{v}: w.{k}" for k, v in weight.items()])
    s_ins_ = f"_from: u._id, _to: v._id, {s_ins_}"
    s_ins = f"          INSERT {{{s_ins_}}} "
    s_last = f"IN {ucol}_{vcol}_edges"
    query0 = s + s_ins + s_last
    return query0


def update_to_numeric(collection_name, field):
    s1 = f"FOR p IN {collection_name} FILTER p.{field} update p with {{"
    s2 = f"{field}: TO_NUMBER(p.{field}) "
    s3 = f"}} in {collection_name}"
    q0 = s1 + s2 + s3
    return q0


def fetch_fields_query(docs, collection_name, match_keys, return_keys):
    """

    :param docs: list of dicts (json-like, ie keys are strings)
    :param collection_name: collection to look up docs
    :param match_keys: keys on which to look for document
    :param return_keys: keys which to return
    :return:
    """

    return_vars = [x.replace("@", "_") for x in return_keys]
    collect_clause = ", ".join(
        [f"{k} = cdoc['{q}']" for k, q in zip(return_vars, return_keys)]
    )
    return_clause = ", ".join(["__i"] + return_vars)
    return_clause = f"{{{return_clause}}}"
    for i, doc in enumerate(docs):
        doc.update({"__i": i})
    if isinstance(docs, list):
        docs = json.dumps(docs)

    match_str = ", ".join(f'"{item}"' for item in match_keys)
    q_update = f"""
        FOR cdoc in {collection_name}
            FOR doc in {docs}
                FILTER MATCHES(cdoc, KEEP(doc, {match_str}))                                             
                    COLLECT __i = doc.__i, {collect_clause}
                    RETURN {return_clause}"""

    return q_update


def get_data_from_cursor(cursor, limit=None):
    batch = []
    cnt = 0
    while True:
        try:
            if limit is not None and cnt >= limit:
                raise StopIteration
            item = next(cursor)
            batch.append(item)
            cnt += 1
        except StopIteration:
            return batch


def fetch_fields_suffix(cursor) -> defaultdict[int, list]:
    data = get_data_from_cursor(cursor)

    map_key: defaultdict[int, list] = defaultdict(list)
    for item in data:
        __i = item.pop("__i")
        map_key[__i] += [item]

    return map_key


def fetch_fields(
    db_client, docs, collection_name, match_keys, return_keys
) -> dict:
    q0 = fetch_fields_query(docs, collection_name, match_keys, return_keys)
    cursor = db_client.execute(q0)
    r = fetch_fields_suffix(cursor)
    return r
