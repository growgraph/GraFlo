import gzip
import json
from collections import defaultdict, ChainMap
from itertools import product

from graph_cast.input.util import parse_vcollection
from graph_cast.util.io import FPSmart
from graph_cast.input.util import define_graphs, update_graph_extra_edges
from graph_cast.architecture.schema import VertexConfig
from graph_cast.architecture.general import transform_foo
from graph_cast.architecture.transform import Transform
from typing import Dict

xml_dummy = "#text"


def apply_mapper(mapper: Dict, document, vertex_config: VertexConfig):
    if "how" in mapper:
        mode = mapper["how"]
        vcol = mapper["name"]
        # value is a dict
        if mode == "dict":
            if (
                ("filter" not in mapper and "unfilter" not in mapper)
                or (
                    "filter" in mapper
                    and all([document[kk] == vv for kk, vv in mapper["filter"].items()])
                )
                or (
                    "unfilter" in mapper
                    and any(
                        [document[kk] != vv for kk, vv in mapper["unfilter"].items()]
                    )
                )
            ):
                kkeys = vertex_config.fields(vcol)

                doc_ = dict()
                if "transforms" in mapper:
                    for t in mapper["transforms"]:
                        t_ = Transform(**t)
                        # doc_.update(transform_foo(t, document))
                        doc_.update(transform_foo(t_, document))

                if "map" in mapper:
                    kkeys += [k for k in mapper["map"] if k not in kkeys]

                if isinstance(document, dict):
                    for kk, vv in document.items():
                        if kk in kkeys:
                            if isinstance(vv, dict):
                                if xml_dummy in vv:
                                    doc_[kk] = vv[xml_dummy]
                            else:
                                doc_[kk] = vv
                else:
                    if not isinstance(document, dict):
                        # logger.warning(mapper)
                        # logger.warning(document)
                        pass
                if "map" in mapper:
                    doc_ = {
                        mapper["map"][k] if k in mapper["map"] else k: v
                        for k, v in doc_.items()
                        if v
                    }
                if "__extra" in mapper:
                    doc_.update(mapper["__extra"])
            else:
                doc_ = dict()
            # if "transforms" in vertex_config[vcol]:
            #     for t in vertex_config[vcol]["transforms"]:
            #         upd = transform_foo(t, doc_)
            #         doc_.update(upd)
            return {vcol: [doc_]}
        elif mode == "value":
            key = mapper["key"]
            return {vcol: [{**{key: document}}]}
        else:
            raise KeyError("Mapper must have map key if it has vertex key")
    # traverse non terminal nodes
    elif "type" in mapper:
        agg = defaultdict(list)
        if "descend_key" in mapper:
            if document and mapper["descend_key"] in document:
                document = document[mapper["descend_key"]]
            else:
                return agg
        if mapper["type"] == "list":
            for cdoc in document:
                for m in mapper["maps"]:
                    item = apply_mapper(m, cdoc, vertex_config)
                    for k, v in item.items():
                        agg[k] += [x for x in v if x]
                    if "edges" in mapper:
                        # check update
                        agg = add_edges(mapper, agg, vertex_config)
            return agg
        elif mapper["type"] == "item":
            for m in mapper["maps"]:
                item = apply_mapper(m, document, vertex_config)
                for k, v in item.items():
                    agg[k] += [x for x in v if x]
            if "edges" in mapper:
                # check update
                agg = add_edges(mapper, agg, vertex_config)
            if "weights" in mapper:
                # check update
                agg = add_weights(mapper, agg)
            if "merge" in mapper:
                for item in mapper["merge"]:
                    agg = smart_merge(
                        agg,
                        item["name"],
                        item["discriminator_key"],
                        item["discriminator_value"],
                    )
            return agg
        else:
            raise ValueError(
                "Mapper type key has incorrect value: list or item allowed"
            )
    else:
        raise KeyError("Mapper type has does not have either how or type keys")


def add_weights(mapper, agg):
    for edge_def in mapper["weights"]:
        source, target = (
            edge_def["source"]["name"],
            edge_def["target"]["name"],
        )
        edges = agg[(source, target)]

        if "vertex" in edge_def:
            for item in edge_def["vertex"]:
                vs = [doc for doc in agg[item["name"]]]
                if "condition" in item.keys():
                    c = item["condition"]
                    vs = [doc for doc in vs if all([q in doc for q in c])]
                if vs:
                    doc = vs[0]
                    if "condition" not in item.keys() or (
                        "condition" in item.keys()
                        and all([doc[k] == v for k, v in c.items()])
                    ):
                        for edoc in edges:
                            edoc["attributes"].update(
                                {item["name"]: doc[item["field"]]}
                            )
        agg[(source, target)] = edges
    return agg


def add_edges(mapper, agg, vertex_config):
    for edge_def in mapper["edges"]:
        # get source and target names
        source, target = (
            edge_def["source"]["name"],
            edge_def["target"]["name"],
        )

        # get source and target edge fields
        source_index, target_index = (
            vertex_config.index(source),
            vertex_config.index(target),
        )

        # get source and target items
        source_items, target_items = agg[source], agg[target]

        if edge_def["how"] == "all":

            source_items = pick_indexed_items_anchor_logic(
                source_items, source_index, edge_def["source"]
            )
            target_items = pick_indexed_items_anchor_logic(
                target_items, target_index, edge_def["target"]
            )

            for u, v in product(source_items, target_items):
                weight = dict()
                if "fields" in edge_def["source"]:
                    for k in edge_def["source"]["fields"]:
                        if k in u:
                            weight[k] = u[k]
                if "fields" in edge_def["target"]:
                    for k in edge_def["target"]["fields"]:
                        if k in v:
                            weight[k] = v[k]
                if "weight_exclusive" in edge_def["source"]:
                    for k in edge_def["source"]["weight_exclusive"]:
                        if k in u:
                            weight[k] = u[k]
                            del u[k]
                if "weight_exclusive" in edge_def["target"]:
                    for k in edge_def["target"]["weight_exclusive"]:
                        if k in v:
                            weight[k] = v[k]
                            del v[k]
                if "values" in edge_def:
                    weight.update({k: v for k, v in edge_def["values"].items()})
                agg[(source, target)] += [
                    {
                        "source": project_dict(u, source_index),
                        "target": project_dict(v, target_index),
                        "attributes": weight,
                    }
                ]
        if edge_def["how"] == "1-n":
            source_field, target_field = (
                edge_def["source"]["field"],
                edge_def["target"]["field"],
            )
            source_items = pick_indexed_items_anchor_logic(
                source_items, source_index, edge_def["source"]
            )
            target_items = pick_indexed_items_anchor_logic(
                target_items, target_index, edge_def["target"]
            )

            target_items = [item for item in target_items if target_field in item]

            if target_items:
                target_items = dict(
                    zip(
                        [item[target_field] for item in target_items],
                        project_dicts(target_items, target_index),
                    )
                )
                for u in source_items:
                    weight = dict()
                    if "fields" in edge_def["source"]:
                        weight.update(
                            {k: u[k] for k in edge_def["source"]["fields"] if k in u}
                        )
                    if "values" in edge_def:
                        weight.update({k: v for k, v in edge_def["values"].items()})
                    up = project_dict(u, source_index)
                    if source_field in u:
                        pointer = u[source_field]
                        if pointer in target_items.keys():
                            agg[(source, target)] += [
                                {
                                    "source": up,
                                    "target": target_items[pointer],
                                    "attributes": weight,
                                }
                            ]
                        else:
                            agg[(source, target)] += [
                                {
                                    "source": up,
                                    "target": v,
                                    "attributes": weight,
                                }
                                for v in target_items.values()
                            ]
                    else:
                        agg[(source, target)] += [
                            {"source": up, "target": v, "attributes": weight}
                            for v in target_items.values()
                        ]
    return agg


def pick_indexed_items_anchor_logic(items, indices, set_spec, anchor_key="anchor"):
    """

    :param items: list of documents (dict)
    :param indices:
    :param set_spec:
    :param anchor_key:
    :return:
    """

    # pick items that have any index field present

    items_ = [item for item in items if any([k in item for k in indices])]

    if anchor_key in set_spec:
        items_ = [
            item
            for item in items_
            if anchor_key in item and item[anchor_key] == set_spec[anchor_key]
        ]
    return items_


def assign_edge_label(edges, label, condition):
    edges_new = [(u, v, label) if condition(u, v) else (u, v, {}) for u, v in edges]
    return edges_new


def clean_arobas(item):
    return {k: v for k, v in item.items() if k[0] != "@"}


def project_dict(item, keys, how="include"):
    if how == "include":
        return {k: v for k, v in item.items() if k in keys}
    elif how == "exclude":
        return {k: v for k, v in item.items() if k not in keys}


def project_dicts(items, keys, how="include"):
    if how == "include":
        return [{k: v for k, v in item.items() if k in keys} for item in items]
    elif how == "exclude":
        return [{k: v for k, v in item.items() if k not in keys} for item in items]
    else:
        raise ValueError(f" `how` should be exclude or include : instead {how}")


def clean_aux_fields(pack):
    pack_out = {}
    for k, cpack in pack.items():
        if k != "@edges":
            pack_out[k] = [clean_arobas(x) for x in cpack]
        else:
            pack_out[k] = [
                (clean_arobas(x[0]), clean_arobas(x[1]), x[2:]) for x in cpack
            ]
    return pack_out


def parse_edges(croot, edge_acc, mapping_fields):
    # TODO push mapping_fields etc to architecture
    """
    extract edge definition and edge fields from definition dict
    :param croot:
    :param edge_acc:
    :param mapping_fields:
    :return:
    """
    if isinstance(croot, dict):
        if "maps" in croot:
            for m in croot["maps"]:
                edge_acc_, mapping_fields = parse_edges(m, edge_acc, mapping_fields)
                edge_acc += edge_acc_
        if "edges" in croot:
            edge_acc_ = []
            for evw in croot["edges"]:
                vname, wname = evw["source"]["name"], evw["target"]["name"]
                vname_fields = None
                wname_fields = None
                edge_def = vname, wname, vname_fields, wname_fields
                edge_acc_ += [edge_def]
                if "field" in evw["source"]:
                    mapping_fields[vname] += [evw["source"]["field"]]
                if "field" in evw["target"]:
                    mapping_fields[wname] += [evw["target"]["field"]]
            return edge_acc_ + edge_acc, mapping_fields
        else:
            return [], defaultdict(list)


def merge_documents(docs, main_key="_key", anchor_key="anchor", anchor_value="main"):
    """
    docs contain docs with main_key and without
    all docs without main_key should be merged with the doc that has doc[anchor_key] == anchor_value
    :param docs:
    :param main_key:
    :param anchor_key:
    :return: list of docs, each of which contains main_key
    """
    mains_, mains, auxs, anchors = [], [], [], []
    # split docs into two groups with and without main_key
    for item in docs:
        (mains_ if main_key in item else auxs).append(item)

    for item in mains_:
        (
            anchors
            if anchor_key in item and item[anchor_key] == anchor_value
            else mains
        ).append(item)

    auxs += anchors
    r = [dict(ChainMap(*auxs))] + mains
    return r


def smart_merge(
    agg, collection_name, discriminant_key="role", discriminant_value="author"
):
    """
    contributor specific merge function

    :param agg:
    :param collection_name:
    :param discriminant_key:
    :param discriminant_value:
    :return:
    """
    wos_standard = defaultdict(list)
    without_standard_heap = []
    seed_list = []
    # split group into 3:
    # standard, non_standard and seed_list
    # merge non_standard onto standard (not replacing fields are already standard item)
    for item in agg[collection_name]:
        if discriminant_key in item:
            if "wos_standard" in item and item[discriminant_key] == discriminant_value:
                wos_standard[item["wos_standard"]] += [item]
            else:
                without_standard_heap += [item]
        else:
            seed_list += [item]

    # heuristics
    for item in without_standard_heap:
        if "display_name" in item:
            split_display_name = item["display_name"].split(", ")
            if len(split_display_name) > 1 and len(split_display_name[1]) > 1:
                last_name, first_name = split_display_name[:2]
            else:
                last_name, first_name = split_display_name[0], ""
        elif "last_name" and "first_name" in item:
            last_name, first_name = item["last_name"], item["first_name"]
        else:
            continue
        if len(first_name) > 0:
            initial = first_name[0]
        else:
            initial = ""
        q = last_name + "," + initial
        for k in wos_standard:
            if q in k:
                wos_standard[k] += [item]

        for k, v in wos_standard.items():
            seed_list += [dict(ChainMap(*v))]
        agg[collection_name] = seed_list
    return agg


def get_json_data(source, pattern=None):
    if source[-2:] == "gz":
        open_foo = gzip.GzipFile
    else:
        open_foo = open

    with open_foo(source, "rb") as fp:
        if pattern:
            fps = FPSmart(fp, pattern)
        else:
            fps = fp
        data = json.load(fps)
    return data


# def foo_parallel(data, kwargs, n=None):
#     func = partial(process_document_top, **kwargs)
#     n_proc = 4
#     if n is not None:
#         data = data[:n]
#     with mp.Pool(n_proc) as p:
#         r = p.map(func, data)
#     return r


def parse_config(config=None):
    """
    only parse_edges depends on json

    :param config:
    :param prefix:
    :return:
    """

    (
        vmap,
        index_fields_dict,
        extra_indices,
        vfields,
        blank_collections,
    ) = parse_vcollection(config)

    edge_def, excl_fields = parse_edges(config["json"], [], defaultdict(list))

    graphs_definition = define_graphs(edge_def, vmap)
    graphs_definition = update_graph_extra_edges(
        graphs_definition, vmap, config["extra_edges"]
    )

    vcollections = list(
        set([graphs_definition[g]["source"] for g in graphs_definition])
        | set([graphs_definition[g]["target"] for g in graphs_definition])
    )
    return (
        vcollections,
        vmap,
        graphs_definition,
        index_fields_dict,
        extra_indices,
    )
