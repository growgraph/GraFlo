import logging

logger = logging.getLogger(__name__)


def parse_vcollection(config):

    # vertex_type -> vertex_collection_name
    vmap = {k: f'{v["basename"]}' for k, v in config["vertex_collections"].items()}

    # vertex_collection_name -> field_definition
    index_fields_dict = {k: v["index"] for k, v in config["vertex_collections"].items()}
    logger.info("index_fields_dict")
    logger.info(f"{index_fields_dict}")
    # vertex_collection_name -> extra_index
    # in addition to index from field_definition
    extra_indices = {
        k: v["extra_index"]
        for k, v in config["vertex_collections"].items()
        if "extra_index" in v
    }

    # vertex_collection_name -> fields_keep
    retrieve_fields_dict = {
        k: v["fields"] for k, v in config["vertex_collections"].items()
    }
    return vmap, index_fields_dict, extra_indices


def derive_graph(edge_def, vmap):
    graph = dict()
    for uv in edge_def:
        u_, v_ = uv[:2]
        u, v = vmap[u_], vmap[v_]

        graph[u_, v_] = {
            "source": u,
            "target": v,
            "edge_name": f"{u}_{v}_edges",
            "graph_name": f"{u}_{v}_graph",
            "type": "direct",
        }
    return graph


def update_graph_extra_edges(graph, vmap, subconfig):
    for item in subconfig:
        u_, v_ = item["source"], item["target"]
        u, v = vmap[u_], vmap[v_]

        graph[u_, v_] = {
            "source": u,
            "target": v,
            "edge_name": f"{u}_{v}_edges",
            "graph_name": f"{u}_{v}_graph",
            "by": vmap[item["by"]],
            "edge_weight": item["edge_weight"],
            "type": "indirect",
        }
    return graph
