import json
import logging

from graph_cast.architecture.schema import Edge
from graph_cast.util.transform import pick_unique_dict

logger = logging.getLogger(__name__)

#
# def create_collection_if_absent(db_client, g, vcol, index, unique=True):
#     if not db_client.has_collection(vcol):
#         _ = g.create_vertex_collection(vcol)
#         general_collection = db_client.collection(vcol)
#         if index is not None and index != ["_key"]:
#             ih = general_collection.add_hash_index(fields=index, unique=unique)
#             return ih
#         else:
#             return None


# def get_arangodb_client(
#     protocol, ip_addr, port, database, cred_name, cred_pass
# ):
#     hosts = f"{protocol}://{ip_addr}:{port}"
#     client = ArangoClient(hosts=hosts)
#
#     sys_db = client.db(database, username=cred_name, password=cred_pass)
#
#     return sys_db


# def delete_collections(sys_db, cnames=(), gnames=(), delete_all=False):
#     logger.info("collections (non system):")
#     logger.info([c for c in sys_db.collections() if c["name"][0] != "_"])
#
#     if delete_all:
#         cnames = [
#             c["name"] for c in sys_db.collections() if c["name"][0] != "_"
#         ]
#         gnames = [g["name"] for g in sys_db.graphs()]
#
#     for cn in cnames:
#         if sys_db.has_collection(cn):
#             sys_db.delete_collection(cn)
#
#     logger.info("collections (after delete operation):")
#     logger.info([c for c in sys_db.collections() if c["name"][0] != "_"])
#
#     logger.info("graphs:")
#     logger.info(sys_db.graphs())
#
#     for gn in gnames:
#         if sys_db.has_graph(gn):
#             sys_db.delete_graph(gn)
#
#     logger.info("graphs (after delete operation):")
#     logger.info(sys_db.graphs())


# def define_vertex_collections(sys_db, graph_config, vertex_index):
#     edges = graph_config.all_edges
#     for u, v in edges:
#         item = graph_config.graph(u, v)
#         gname = item["graph_name"]
#         logger.info(f'{item["source"]}, {item["target"]}, {gname}')
#         if sys_db.has_graph(gname):
#             g = sys_db.graph(gname)
#         else:
#             g = sys_db.create_graph(gname)
#         # TODO create collections without referencing the graph
#         ih = create_collection_if_absent(
#             sys_db,
#             g,
#             item["source"],
#             vertex_index(u),
#         )
#
#         ih = create_collection_if_absent(
#             sys_db,
#             g,
#             item["target"],
#             vertex_index(v),
#         )


# def define_edge_collections(sys_db, graph_config):
#     edges = graph_config.all_edges
#     for u, v in edges:
#         item = graph_config.graph(u, v)
#         gname = item["graph_name"]
#         if sys_db.has_graph(gname):
#             g = sys_db.graph(gname)
#         else:
#             g = sys_db.create_graph(gname)
#         if not g.has_edge_definition(item["edge_name"]):
#             _ = g.create_edge_definition(
#                 edge_collection=item["edge_name"],
#                 from_vertex_collections=[item["source"]],
#                 to_vertex_collections=[item["target"]],
#             )


# def define_vertex_indices(sys_db, vertex_config):
#     for c in vertex_config.collections:
#         for index_dict in vertex_config.extra_index_list(c):
#             general_collection = sys_db.collection(
#                 vertex_config.vertex_dbname(c)
#             )
#             ih = general_collection.add_hash_index(
#                 fields=index_dict["fields"], unique=index_dict["unique"]
#             )


# def define_edge_indices(sys_db, graph_config):
#     for u, v in graph_config.all_edges:
#         item = graph_config.graph(u, v)
#         if "index" in item:
#             for index_dict in item["index"]:
#                 general_collection = sys_db.collection(item["edge_name"])
#                 ih = general_collection.add_hash_index(
#                     fields=index_dict["fields"], unique=index_dict["unique"]
#                 )


# def define_collections_and_indices(sys_db, graph_config, vertex_config):
#     define_vertex_collections(sys_db, graph_config, vertex_config.index)
#     define_edge_collections(sys_db, graph_config)
#     # TODO add indices if absent
#     define_vertex_indices(sys_db, vertex_config)
#     define_edge_indices(sys_db, graph_config)


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
    upsert_weight_vertex=None,
    upsert_weight_fields=None,
):
    """

    :param docs_edges: in format  [{'__source': source_doc, '__target': target_doc}]
    :param source_collection_name,
    :param target_collection_name,
    :param edge_col_name:
    :param match_keys_source:
    :param match_keys_target:
    :param filter_uniques:
    :param upsert_weight_vertex: upsert based on weight, rather than add new edges connecting the same vertices

    :return:
    """
    if isinstance(docs_edges, list):
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
        source_filter = f"""
                            LET sources = (
                                FOR v IN {source_collection_name}
                                  FILTER {filter_source} LIMIT 1
                                  RETURN v)"""

    if match_keys_target[0] == "_key":
        result_to = f'CONCAT("{target_collection_name}/", edge.__target._key)'
        target_filter = ""
    else:
        result_to = "targets[0]._id"
        filter_target = " && ".join(
            [f"v.{k} == edge.__target.{k}" for k in match_keys_target]
        )
        target_filter = f"""
                            LET targets = (
                                FOR v IN {target_collection_name}
                                  FILTER {filter_target} LIMIT 1
                                  RETURN v)"""

    result = (
        f"MERGE({{_from : {result_from}, _to : {result_to}}},"
        " UNSET(edge, '__source', '__target'))"
    )

    if upsert_weight_vertex is not None and upsert_weight_vertex:
        weights = ", " + ", ".join(
            [f"'{k}': edge.{k}" for k in upsert_weight_vertex]
        )
        ups_edge = (
            f"{{'_from': sources[0]._id, '_to': targets[0]._id{weights}}}"
        )

        upsert_clause = f"UPSERT {ups_edge}"
        update_clause = f"UPDATE {{}}"
    else:
        upsert_clause = ""
        update_clause = ""

    q_update = f"""
        FOR edge in {docs_edges} {source_filter} {target_filter}
            LET doc = {result}
            {upsert_clause} INSERT doc {update_clause} 
                IN {edge_col_name} OPTIONS {{ exclusive: true }}"""
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
