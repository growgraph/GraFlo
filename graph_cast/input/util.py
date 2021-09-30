import importlib
import logging

logger = logging.getLogger(__name__)


def parse_vcollection(config, conf_obj):

    # vertex_type -> vertex_collection_name
    conf_obj.name = {
        k: f'{v["basename"]}' for k, v in config["vertex_collections"].items()
    }

    # vertex_collection_name -> indices
    conf_obj.index = {
        k: v["index"] if "index" in v else ["_key"]
        for k, v in config["vertex_collections"].items()
    }
    logger.info("index_fields_dict")
    logger.info(f"{conf_obj.index}")

    # vertex_collection_name -> extra_index
    # in addition to index from field_definition
    conf_obj.extra_indices = {
        k: v["extra_index"]
        for k, v in config["vertex_collections"].items()
        if "extra_index" in v
    }

    # vertex_collection_name -> fields
    conf_obj.fields = {
        k: (v["fields"] if "fields" in v else [])
        for k, v in config["vertex_collections"].items()
    }

    conf_obj.blank_collections = [
        k
        for k, v in config["vertex_collections"].items()
        if "extra" in v and "blank" in v["extra"]
    ]

    conf_obj.numeric_fields_list = {
        k: v["numeric_fields"]
        for k, v in config["vertex_collections"].items()
        if "numeric_fields" in v
    }


def define_graphs(edge_def, vmap):
    graphs_definition = dict()
    for item in edge_def:
        u_, v_ = item["source"], item["target"]
        u, v = vmap(u_), vmap(v_)

        graphs_definition[u_, v_] = {
            "source": u,
            "target": v,
            "edge_name": f"{u}_{v}_edges",
            "graph_name": f"{u}_{v}_graph",
            "type": "direct",
        }
        if "index" in item:
            graphs_definition[u_, v_]["index"] = item["index"]
    return graphs_definition


def update_graph_extra_edges(graphs_definition, vmap, subconfig):
    for item in subconfig:
        u_, v_ = item["source"], item["target"]
        u, v = vmap(u_), vmap(v_)

        graphs_definition[u_, v_] = {
            "source": u,
            "target": v,
            "edge_name": f"{u}_{v}_edges",
            "graph_name": f"{u}_{v}_graph",
            "by": vmap(item["by"]),
            "edge_weight": item["edge_weight"],
            "type": "indirect",
        }
        if "index" in item:
            graphs_definition[u_, v_]["index"] = item["index"]
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
