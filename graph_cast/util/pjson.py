from collections import defaultdict
from graph_cast.utils_json import parse_edges


def parse_config(config=None, prefix="toy_"):
    """
    only parse_edges depends on json

    :param config:
    :param prefix:
    :return:
    """
    # vertex_type -> vertex_collection_name
    vmap = {
        k: f'{prefix}{v["basename"]}' for k, v in config["vertex_collections"].items()
    }

    # vertex_collection_name -> field_definition
    index_fields_dict = {k: v["index"] for k, v in config["vertex_collections"].items()}

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

    edge_def, excl_fields = parse_edges(config["json"], [], defaultdict(list))
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

    for item in config["extra_edges"]:
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
    vcollections = list(
        set([graph[g]["source"] for g in graph])
        | set([graph[g]["target"] for g in graph])
    )
    return vcollections, vmap, graph, index_fields_dict, extra_indices
