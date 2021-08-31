import json
from arango import ArangoClient
import logging

from graph_cast.util.transform import pick_unique_dict

logger = logging.getLogger(__name__)


def create_collection_if_absent(db_client, g, vcol, index, unique=True):
    if not db_client.has_collection(vcol):
        _ = g.create_vertex_collection(vcol)
        general_collection = db_client.collection(vcol)
        ih = general_collection.add_hash_index(fields=index, unique=unique)
        return ih


def fetch_collection(db_client, collection_name, erase_existing=False):
    if db_client.has_collection(collection_name):
        if erase_existing:
            db_client.delete_collection(collection_name)
        else:
            collection = db_client.collection(collection_name)
    else:
        collection = db_client.create_collection(collection_name)
    return collection


def get_arangodb_client(protocol, ip_addr, port, database, cred_name, cred_pass):
    hosts = f"{protocol}://{ip_addr}:{port}"
    client = ArangoClient(hosts=hosts)

    sys_db = client.db(database, username=cred_name, password=cred_pass)

    return sys_db


def delete_collections(sys_db, cnames=(), gnames=(), delete_all=False):

    logger.info("collections (non system):")
    logger.info([c for c in sys_db.collections() if c["name"][0] != "_"])

    if delete_all:
        cnames = [c["name"] for c in sys_db.collections() if c["name"][0] != "_"]
        gnames = [g["name"] for g in sys_db.graphs()]

    for cn in cnames:
        if sys_db.has_collection(cn):
            sys_db.delete_collection(cn)

    logger.info("collections (after delete operation):")
    logger.info([c for c in sys_db.collections() if c["name"][0] != "_"])

    logger.info("graphs:")
    logger.info(sys_db.graphs())

    for gn in gnames:
        if sys_db.has_graph(gn):
            sys_db.delete_graph(gn)

    logger.info("graphs (after delete operation):")
    logger.info(sys_db.graphs())


def define_vertex_collections(sys_db, graphs, index_fields_dict):
    for uv, item in graphs.items():
        u, v = uv
        gname = item["graph_name"]
        logger.info(f'{item["source"]}, {item["target"]}, {gname}')
        if sys_db.has_graph(gname):
            g = sys_db.graph(gname)
        else:
            g = sys_db.create_graph(gname)
        # TODO create collections without referencing the graph
        ih = create_collection_if_absent(
            sys_db, g, item["source"], index_fields_dict[u]
        )

        ih = create_collection_if_absent(
            sys_db, g, item["target"], index_fields_dict[v]
        )


def define_edge_collections(sys_db, graphs):
    for uv, item in graphs.items():
        gname = item["graph_name"]
        if sys_db.has_graph(gname):
            g = sys_db.graph(gname)
        else:
            g = sys_db.create_graph(gname)
        if not g.has_edge_definition(item["edge_name"]):
            _ = g.create_edge_definition(
                edge_collection=item["edge_name"],
                from_vertex_collections=[item["source"]],
                to_vertex_collections=[item["target"]],
            )


def define_vertex_indices(sys_db, vmap, extra_index):
    for cname, list_indices in extra_index.items():
        for index_dict in list_indices:
            general_collection = sys_db.collection(vmap[cname])
            ih = general_collection.add_hash_index(
                fields=index_dict["fields"], unique=index_dict["unique"]
            )


def define_edge_indices(sys_db, graphs):
    for uv, item in graphs.items():
        general_collection = sys_db.collection(item["edge_name"])
        for index_dict in item["index"]:
            ih = general_collection.add_hash_index(
                fields=index_dict["fields"], unique=index_dict["unique"]
            )


def define_collections_and_indices(
    sys_db, graphs, vmap, index_fields_dict, extra_index
):
    define_vertex_collections(sys_db, graphs, index_fields_dict)
    define_edge_collections(sys_db, graphs)
    define_vertex_indices(sys_db, vmap, extra_index)


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

    :param docs: list of dictionaries (json-like, ie keys are strings)
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
    upsert_line = ", ".join([f'"{k}": doc.{k}' for k in match_keys])
    upsert_line = f"{{{upsert_line}}}"

    if isinstance(update_keys, list):
        update_line = ", ".join([f'"{k}": doc.{k}' for k in update_keys])
        update_line = f"{{{update_line}}}"
    elif update_keys == "doc":
        update_line = "doc"
    else:
        update_line = "{}"
    q_update = f"""FOR doc in {docs}
                        UPSERT {upsert_line}
                        INSERT doc
                        UPDATE {update_line} in {collection_name}"""
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

    :param docs_edges: in format  [{'source': source_doc, 'target': target_doc}]
    :param source_collection_name,
    :param target_collection_name,
    :param edge_col_name:
    :param match_keys_source:
    :param match_keys_target:
    :param filter_uniques:

    :return:
    """
    example = docs_edges[0]
    if isinstance(docs_edges, list):
        if filter_uniques:
            docs_edges = pick_unique_dict(docs_edges)
        docs_edges = json.dumps(docs_edges)

    if match_keys_source[0] == "_key":
        result_from = f'CONCAT("{source_collection_name}/", edge.source._key)'
        source_filter = ""
    else:
        result_from = "sources[0]._id"
        filter_source = " && ".join(
            [f"v.{k} == edge.source.{k}" for k in match_keys_source]
        )
        source_filter = f"""
                            LET sources = (
                                FOR v IN {source_collection_name}
                                  FILTER {filter_source} LIMIT 1
                                  RETURN v)"""

    if match_keys_target[0] == "_key":
        result_to = f'CONCAT("{target_collection_name}/", edge.target._key)'
        target_filter = ""
    else:
        result_to = "targets[0]._id"
        filter_target = " && ".join(
            [f"v.{k} == edge.target.{k}" for k in match_keys_target]
        )
        target_filter = f"""
                            LET targets = (
                                FOR v IN {target_collection_name}
                                  FILTER {filter_target} LIMIT 1
                                  RETURN v)"""

    if "attributes" in example.keys() and example["attributes"]:
        result = f"MERGE({{_from : {result_from}, _to : {result_to}}}, edge.attributes)"
        # result = f"{{_from : {result_from}, _to : {result_to}, attributes: edge.attributes}}"
    else:
        result = f"{{_from : {result_from}, _to : {result_to}}}"

    q_update = f"""
        FOR edge in {docs_edges} {source_filter} {target_filter}
            INSERT {result} in {edge_col_name}"""
    return q_update


def define_extra_edges(g):
    """
    g create a query from u to v by w : u -> w -> v and add properties of w as properties of the edge

    {
        "source": u,
        "target": v,
        "by": w,
        "edge_name": ecollection_name,
        "edge_weight": item["edge_weight"],
        "type": "indirect"
    }

    :param g:
    :return:
    """
    ucol, vcol, wcol = g["source"], g["target"], g["by"]
    edge_weight = g["edge_weight"]
    s = (
        f"FOR w IN {wcol}"
        f"  LET uset = (FOR u IN 1..1 INBOUND w {ucol}_{wcol}_edges RETURN u)"
        f"  LET vset = (FOR v IN 1..1 INBOUND w {vcol}_{wcol}_edges RETURN v)"
        f"  FOR u in uset"
        f"      FOR v in vset"
    )
    s_ins_ = ", ".join([f"{v}: w.{k}" for k, v in edge_weight.items()])
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
