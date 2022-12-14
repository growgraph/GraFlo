import json
import logging

from graph_cast.architecture.schema import Edge
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
    q_update = f"""FOR doc in {docs}
                        UPSERT {upsert_clause}
                        INSERT doc
                        UPDATE {update_clause} 
                            IN {collection_name} OPTIONS {{ exclusive: true }}"""
    return q_update


def insert_edges_batch(
    docs_edges,
    source_collection_name,
    target_collection_name,
    edge_col_name,
    match_keys_source=("_key",),
    match_keys_target=("_key",),
    filter_uniques=True,
):
    """

    :param docs_edges: in format  [{'__source': source_doc, '__target': target_doc}]
    :param source_collection_name,
    :param target_collection_name,
    :param edge_col_name:
    :param match_keys_source:
    :param match_keys_target:
    :param filter_uniques:

    :return:
    """

    if isinstance(docs_edges, list):
        if docs_edges:
            logger.info(f" docs_edges[0] = {docs_edges[0]}")
        if filter_uniques:
            docs_edges = pick_unique_dict(docs_edges)
        docs_edges = json.dumps(docs_edges)

    if match_keys_source[0] == "_key":
        result_from = (
            f'CONCAT("{source_collection_name}/", edge.__source._key)'
        )
        source_filter = ""
    else:
        result_from = "sources[0]._id"
        filter_source = " && ".join(
            [f"v.{k} == edge.__source.{k}" for k in match_keys_source]
        )
        source_filter = (
            f"LET sources = (FOR v IN {source_collection_name} FILTER"
            f" {filter_source} LIMIT 1 RETURN v)"
        )

    if match_keys_target[0] == "_key":
        result_to = f'CONCAT("{target_collection_name}/", edge.__target._key)'
        target_filter = ""
    else:
        result_to = "targets[0]._id"
        filter_target = " && ".join(
            [f"v.{k} == edge.__target.{k}" for k in match_keys_target]
        )
        target_filter = (
            f"LET targets = (FOR v IN {target_collection_name} FILTER"
            f" {filter_target} LIMIT 1 RETURN v)"
        )

    result = (
        f"MERGE({{_from : {result_from}, _to : {result_to}}},"
        " UNSET(edge, '__source', '__target'))"
    )
    ups_from = result_from if source_filter else "doc._from"
    ups_to = result_to if target_filter else "doc._to"
    upsert = (
        f"{{'_from': {ups_from}, '_to': {ups_to}, 'publication':"
        " edge.publication}"
    )
    logger.info(f" source_filter = {source_filter}")
    logger.info(f" target_filter = {target_filter}")
    logger.info(f" doc = {result}")
    logger.info(f" upsert clause: {upsert}")
    q_update = f"""
        FOR edge in {docs_edges} {source_filter} {target_filter}
            LET doc = {result}
            UPSERT {upsert}
            INSERT doc
            UPDATE {{}}
            in {edge_col_name} OPTIONS {{ exclusive: true }}"""
    return q_update


def define_extra_edges(g: Edge):
    """
    g create a query from u to v by w : u -> w -> v and add properties of w as properties of the edge


    :param g:
    :return:
    """
    ucol, vcol, wcol = g.source, g.target, g.by
    weight = g.weight_dict
    s = (
        f"FOR w IN {wcol}"
        f"  LET uset = (FOR u IN 1..1 INBOUND w {ucol}_{wcol}_edges RETURN u)"
        f"  LET vset = (FOR v IN 1..1 INBOUND w {vcol}_{wcol}_edges RETURN v)"
        "  FOR u in uset"
        "      FOR v in vset"
    )
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
