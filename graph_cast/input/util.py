import importlib
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
    vfields = {k: v["fields"] for k, v in config["vertex_collections"].items()}
    return vmap, index_fields_dict, extra_indices, vfields


def define_graphs(edge_def, vmap):
    graphs_definition = dict()
    for item in edge_def:
        u_, v_ = item["source"], item["target"]
        u, v = vmap[u_], vmap[v_]

        graphs_definition[u_, v_] = {
            "source": u,
            "target": v,
            "edge_name": f"{u}_{v}_edges",
            "graph_name": f"{u}_{v}_graph",
            "type": "direct",
        }
    return graphs_definition


def update_graph_extra_edges(graphs_definition, vmap, subconfig):
    for item in subconfig:
        u_, v_ = item["source"], item["target"]
        u, v = vmap[u_], vmap[v_]

        graphs_definition[u_, v_] = {
            "source": u,
            "target": v,
            "edge_name": f"{u}_{v}_edges",
            "graph_name": f"{u}_{v}_graph",
            "by": vmap[item["by"]],
            "edge_weight": item["edge_weight"],
            "type": "indirect",
        }
    return graphs_definition


def transform_foo(transform, doc):
    if "module" in transform:
        module = importlib.import_module(transform["module"])
    elif "class" in transform:
        module = eval(transform["class"])
    else:
        raise KeyError("Either module or class keys should be present")
    try:
        foo = getattr(module, transform["foo"])
        if "input" in transform:
            if "output" in transform:
                args = [doc[k] for k in transform["input"]]
                upd = {k: v for k, v in zip(transform["output"], foo(*args))}
            else:
                args = [doc[k] for k in transform["input"]]
                upd = foo(*args)
        elif "fields" in transform:
            upd = {k: foo(v) for k, v in doc.items() if k in transform["fields"]}
    except:
        upd = {}
    return upd
