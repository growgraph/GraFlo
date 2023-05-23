import json
import logging
from collections import defaultdict

from graph_cast.architecture.schema import Edge, _source_aux, _target_aux
from graph_cast.util.transform import pick_unique_dict

logger = logging.getLogger(__name__)


def insert_return_batch(docs, collection_name):
    docs = json.dumps(docs)
    query0 = f"""FOR doc in {docs}
          INSERT doc
          INTO {collection_name}
          LET inserted = NEW
          RETURN {{_key: inserted._key}}
    """
    return query0


def upsert_docs_batch(
    docs, collection_name, match_keys, update_keys=None, filter_uniques=True
):
    """

    :param docs: list of dicts (json-like, ie keys are strings)
    :param collection_name: collection where to upsert
    :param match_keys: keys on which to look for document
    :param update_keys: keys which to update if doc in the collection, if update_keys='doc', update all
    :param filter_uniques:
    :return:
    """

    if isinstance(docs, list):
        if filter_uniques:
            docs = pick_unique_dict(docs)
        docs = json.dumps(docs)
    upsert_clause = ", ".join([f'"{k}": doc.{k}' for k in match_keys])
    upsert_clause = f"{{{upsert_clause}}}"

    if isinstance(update_keys, list):
        update_clause = ", ".join([f'"{k}": doc.{k}' for k in update_keys])
        update_clause = f"{{{update_clause}}}"
    elif update_keys == "doc":
        update_clause = "doc"
    else:
        update_clause = "{}"

    options = "OPTIONS {exclusive: true, ignoreErrors: true}"

    q_update = f"""FOR doc in {docs}
                        UPSERT {upsert_clause}
                        INSERT doc
                        UPDATE {update_clause} 
                            IN {collection_name} {options}"""
    return q_update


def insert_edges_batch(
    docs_edges,
    source_collection_name,
    target_collection_name,
    edge_col_name,
    match_keys_source=("_key",),
    match_keys_target=("_key",),
    filter_uniques=True,
    uniq_weight_fields=None,
    uniq_weight_collections=None,
    upsert_option=False,
    head=None,
):
    f"""
        using ("_key",) for match_keys_source and match_keys_target saves time
            (no need to look it up from field discriminants)

    :param docs_edges: in format  [{{ _source_aux: source_doc, _target_aux: target_doc}}]
    :param source_collection_name,
    :param target_collection_name,
    :param edge_col_name:
    :param match_keys_source:
    :param match_keys_target:
    :param filter_uniques:
    :param uniq_weight_fields
    :param uniq_weight_collections
    :param upsert_option
    :param head: keep head docs

    :return:
    """

    if isinstance(docs_edges, list):
        if docs_edges:
            logger.info(f" docs_edges[0] = {docs_edges[0]}")
        if head is not None:
            docs_edges = docs_edges[:head]
        if filter_uniques:
            docs_edges = pick_unique_dict(docs_edges)
        docs_edges_str = json.dumps(docs_edges)
    else:
        return ""

    if match_keys_source[0] == "_key":
        result_from = (
            f'CONCAT("{source_collection_name}/", edge.{_source_aux}._key)'
        )
        source_filter = ""
    else:
        result_from = "sources[0]._id"
        filter_source = " && ".join(
            [f"v.{k} == edge.{_source_aux}.{k}" for k in match_keys_source]
        )
        source_filter = (
            f"LET sources = (FOR v IN {source_collection_name} FILTER"
            f" {filter_source} LIMIT 1 RETURN v)"
        )

    if match_keys_target[0] == "_key":
        result_to = (
            f'CONCAT("{target_collection_name}/", edge.{_target_aux}._key)'
        )
        target_filter = ""
    else:
        result_to = "targets[0]._id"
        filter_target = " && ".join(
            [f"v.{k} == edge.{_target_aux}.{k}" for k in match_keys_target]
        )
        target_filter = (
            f"LET targets = (FOR v IN {target_collection_name} FILTER"
            f" {filter_target} LIMIT 1 RETURN v)"
        )

    doc_definition = (
        f"MERGE({{_from : {result_from}, _to : {result_to}}},"
        f" UNSET(edge, '{_source_aux}', '{_target_aux}'))"
    )

    logger.info(f" source_filter = {source_filter}")
    logger.info(f" target_filter = {target_filter}")
    logger.info(f" doc = {doc_definition}")

    if upsert_option:
        ups_from = result_from if source_filter else "doc._from"
        ups_to = result_to if target_filter else "doc._to"

        weight_fs = []
        weight_fs += (
            uniq_weight_fields if uniq_weight_fields is not None else []
        )
        weight_fs += (
            uniq_weight_collections
            if uniq_weight_collections is not None
            else []
        )
        if weight_fs:
            weights_clause = ", " + ", ".join(
                [f"'{x}' : edge.{x}" for x in weight_fs]
            )
        else:
            weights_clause = ""

        upsert = (
            f"{{'_from': {ups_from}, '_to': {ups_to}" + weights_clause + "}"
        )
        logger.info(f" upsert clause: {upsert}")
        clauses = f"UPSERT {upsert} INSERT doc UPDATE {{}}"
        options = "OPTIONS {exclusive: true}"
    else:
        clauses = "INSERT doc"
        options = "OPTIONS {exclusive: true, ignoreErrors: true}"

    q_update = f"""
        FOR edge in {docs_edges_str} {source_filter} {target_filter}
            LET doc = {doc_definition}
            {clauses}
            in {edge_col_name} {options}"""
    return q_update


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
