from itertools import permutations, product
from collections import defaultdict, ChainMap
from os import listdir
from os.path import isfile, join

import pandas as pd

from graph_cast.util.io import Chunker, ChunkerDataFrame
from graph_cast.architecture.config import Configurator

from graph_cast.arango.util import (
    upsert_docs_batch,
    insert_edges_batch,
    insert_return_batch,
)
from graph_cast.input.util import (
    transform_foo,
    parse_vcollection,
    define_graphs,
    update_graph_extra_edges,
)


def parse_edges(config):
    edges = config["edge_collections"].copy()
    if "extra_edges" in config:
        extra_edges = config["extra_edges"].copy()
    else:
        extra_edges = {}
    return edges, extra_edges


def parse_input_output_field_map(subconfig):
    """
    extract map of observed fields in the table to the fields in the collection
    :param subconfig:
    :return:
    """
    field_maps = {}
    for item in subconfig:
        field_maps[item["tabletype"]] = [
            {"type": vc["type"], "map": vc["map_fields"]}
            for vc in item["vertex_collections"]
            if "map_fields" in vc
        ]
    return field_maps


def parse_transformations(subconfig):
    transform_maps = {}
    for item in subconfig:
        if "transforms" in item:
            transform_maps[item["tabletype"]] = item["transforms"]
    return transform_maps


def parse_encodings(subconfig):
    encodings_map = {}
    for item in subconfig:
        if "encoding" in item:
            encodings_map[item["tabletype"]] = item["encoding"]
        else:
            encodings_map[item["tabletype"]] = None
    return encodings_map


def parse_logic(subconfig):
    logic_maps = {}
    for item in subconfig:
        if "logic" in item:
            logic_maps[item["tabletype"]] = item["logic"]
        else:
            logic_maps[item["tabletype"]] = None
    return logic_maps


def parse_graph(config, conf_obj):
    edges, extra_edges = parse_edges(config)

    graphs_def = define_graphs(edges, conf_obj.vertex_config.name)
    conf_obj.graphs_def = update_graph_extra_edges(
        graphs_def, conf_obj.vertex_config.name, extra_edges
    )


def parse_modes2graphs(subconfig, conf_obj):
    graph = conf_obj.graphs_def
    modes2graphs = defaultdict(list)
    modes2collections = {}
    direct_graph = {k: v for k, v in graph.items() if v["type"] == "direct"}

    for item in subconfig:
        table_type = item["tabletype"]

        vcols = [iitem["type"] for iitem in item["vertex_collections"]]
        # here transform into standard form [{"collection": col_name, "map" map}]
        # from [{"collection": col_name, "maps" maps}] (where many maps are applied)
        modes2collections[table_type] = item["vertex_collections"]
        for u, v in permutations(vcols, 2):
            if (u, v) in direct_graph:
                modes2graphs[table_type] += [(u, v)]

    conf_obj.modes2graphs = {k: list(set(v)) for k, v in modes2graphs.items()}
    conf_obj.modes2collections = modes2collections


def parse_weights(subconfig, conf_obj):
    weights_definition = {}
    for item in subconfig:
        weights_definition[item["tabletype"]] = item["weights"]
    conf_obj.weights_definition = weights_definition


def discover_files(modes, fpath, limit_files=None):
    files_dict = {}

    for keyword in modes:
        files_dict[keyword] = sorted(
            [
                join(fpath, f)
                for f in listdir(fpath)
                if isfile(join(fpath, f)) and keyword in f
            ]
        )

    if limit_files:
        files_dict = {k: v[:limit_files] for k, v in files_dict.items()}
    return files_dict


def table_to_vcollections(
    rows,
    header_dict,
    conf,
):

    vdocs = defaultdict(list)
    edocs = defaultdict(list)
    weights = {}
    vertex_conf = conf.vertex_config

    # perform possible transforms

    rows_raw = [{k: item[v] for k, v in header_dict.items()} for item in rows]

    transformation_outputs = set(
        [
            item
            for transformation in conf.current_transformations
            for item in transformation["output"]
        ]
    )
    rows_working = []

    for doc in rows_raw:
        transformed = [
            transform_foo(transformation, doc)
            for transformation in conf.current_transformations
        ]
        doc_upd = {**doc, **dict(ChainMap(*transformed))}
        rows_working += [doc_upd]

    for ccitem in conf.current_collections:
        vdoc_acc = []
        vcol = ccitem["type"]

        current_fields = set(vertex_conf.index(vcol)) | set(
            vertex_conf.vfields[vcol] if vcol in vertex_conf.vfields else {}
        )

        default_input = current_fields & (
            transformation_outputs | set(header_dict.keys())
        )

        if default_input:
            vdoc_acc += [[{f: item[f] for f in default_input} for item in rows_working]]

        if "map_fields" in ccitem:
            vdoc_acc += [
                [
                    {v: item[k] for k, v in ccitem["map_fields"].items()}
                    for item in rows_working
                ]
            ]
        vdocs[vcol].append([dict(ChainMap(*auxs)) for auxs in zip(*vdoc_acc)])

    for wdef in conf.current_weights:
        for edges_def in wdef["edge_collections"]:
            u, v = edges_def["source"]["name"], edges_def["target"]["name"]
            cfields = edges_def["fields"]
            weights[(u, v)] = [{f: item[f] for f in cfields} for item in rows_working]

    # if blank collection has no aux fields - inflate it
    for vcol in vertex_conf.blank_collections:
        # if blank collection is in vdocs - inflate it, otherwise - create
        if vcol in vdocs:
            for j, docs in enumerate(vdocs[vcol]):
                if not docs:
                    vdocs[vcol][j] = [{}] * len(rows)
        else:
            vdocs[vcol].append([{}] * len(rows))

    for u, v in conf.current_graphs:
        g = u, v
        if (
            u not in vertex_conf.blank_collections
            and v not in vertex_conf.blank_collections
        ):
            if conf.graphs_def[u, v]["type"] == "direct":
                if u != v:
                    for ubatch, vbatch in product(vdocs[u], vdocs[v]):
                        ebatch = [
                            {"source": x, "target": y} for x, y in zip(ubatch, vbatch)
                        ]
                        if g in weights:
                            ebatch = [
                                {**item, **{"attributes": attr}}
                                for item, attr in zip(ebatch, weights[g])
                            ]
                        edocs[g].extend(ebatch)
                else:
                    for ubatch, vbatch in permutations(vdocs[u]):
                        ebatch = [
                            {"source": x, "target": y}
                            for x, y, attr in zip(ubatch, ubatch)
                        ]
                        if g in weights:
                            ebatch = [
                                {**item, **{"attributes": attr}}
                                for item, attr in zip(ebatch, weights[g])
                            ]
                        edocs[g].extend(ebatch)

    return vdocs, edocs


def process_table(tabular_resource, batch_size, max_lines, db_client, conf):

    if isinstance(tabular_resource, pd.DataFrame):
        chk = ChunkerDataFrame(tabular_resource, batch_size, max_lines)
    elif isinstance(tabular_resource, str):
        chk = Chunker(tabular_resource, batch_size, max_lines, encoding=conf.encoding)
    else:
        raise TypeError(f"tabular_resource type is not str or pd.DataFrame")
    header = chk.pop_header()
    header_dict = dict(zip(header, range(len(header))))

    while not chk.done:
        lines = chk.pop()
        if lines:
            vdocuments, edocuments = table_to_vcollections(
                lines,
                header_dict,
                conf,
            )

            # TODO move db related stuff out
            for vcol, batches in vdocuments.items():
                for j, data in enumerate(batches):
                    # blank nodes: push and get back their keys  {"_key": ...}
                    if vcol in conf.vertex_config.blank_collections:
                        query0 = insert_return_batch(
                            data, conf.vertex_config.name(vcol)
                        )
                        cursor = db_client.aql.execute(query0)
                        vdocuments[vcol][j] = [item for item in cursor]
                    else:
                        query0 = upsert_docs_batch(
                            data,
                            conf.vertex_config.name(vcol),
                            conf.vertex_config.index(vcol),
                            "doc",
                            True,
                        )
                        cursor = db_client.aql.execute(query0)

            # update edge data with blank node edges
            for vcol in conf.vertex_config.blank_collections:
                for vfrom, vto in conf.current_graphs:
                    if vcol == vfrom or vcol == vto:
                        for from_batch, to_batch in product(
                            vdocuments[vfrom], vdocuments[vto]
                        ):
                            edocuments[(vfrom, vto)].extend(
                                [
                                    {"source": x, "target": y}
                                    for x, y in zip(from_batch, to_batch)
                                ]
                            )

            for (vfrom, vto), data in edocuments.items():
                query0 = insert_edges_batch(
                    data,
                    conf.vertex_config.name(vfrom),
                    conf.vertex_config.name(vto),
                    conf.graphs_def[vfrom, vto]["edge_name"],
                    conf.vertex_config.index(vfrom),
                    conf.vertex_config.index(vto),
                    False,
                )
                cursor = db_client.aql.execute(query0)


def prepare_config(config):

    conf_obj = Configurator(config)

    # vertex_collection -> (table field -> collection field)

    # table_type -> [ {vertex_collection :vc, map: (table field -> collection field)} ]
    conf_obj.table_collection_maps = parse_input_output_field_map(config["csv"])

    conf_obj.transformation_maps = parse_transformations(config["csv"])

    conf_obj.encodings = parse_encodings(config["csv"])

    conf_obj.logic = parse_logic(config["csv"])

    parse_graph(config, conf_obj)

    parse_modes2graphs(config["csv"], conf_obj)

    parse_weights(config["csv"], conf_obj)

    return conf_obj
