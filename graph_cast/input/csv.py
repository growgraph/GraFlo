from itertools import permutations
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
        field_maps[item["tabletype"]] = {
            vc["type"]: vc["map_fields"]
            for vc in item["vertex_collections"]
            if "map_fields" in vc
        }
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

    graphs_def = define_graphs(edges, conf_obj.vmap)
    conf_obj.graphs_def = update_graph_extra_edges(
        graphs_def, conf_obj.vmap, extra_edges
    )


def parse_modes2graphs(subconfig, conf_obj):
    graph = conf_obj.graphs_def
    modes2graphs = defaultdict(list)
    modes2collections = dict()
    direct_graph = {k: v for k, v in graph.items() if v["type"] == "direct"}

    for item in subconfig:
        ftype = item["tabletype"]

        vcols = [iitem["type"] for iitem in item["vertex_collections"]]
        modes2collections[ftype] = list(set(vcols))
        for u, v in permutations(vcols, 2):
            if (u, v) in direct_graph:
                modes2graphs[ftype] += [(u, v)]

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

    vdocs = {}
    edocs = {}
    weights = {}

    # do possible transforms

    fields_extracted_raw = [
        {k: item[v] for k, v in header_dict.items()} for item in rows
    ]

    transformed_fields = {}

    for transformation in conf.current_transformations:
        inputs = transformation["input"]
        outputs = transformation["output"]
        transformed_fields[(tuple(inputs), tuple(outputs))] = [
            transform_foo(transformation, doc) for doc in fields_extracted_raw
        ]

    for vcol in conf.current_collections:
        current_fields = set(
            conf.index_fields_dict[vcol] if vcol in conf.index_fields_dict else {}
        ) | set(
            conf.current_field_maps[vcol] if vcol in conf.current_field_maps else {}
        )

        # keys that do not require transformations
        default_input = current_fields & set(header_dict.keys())
        vdoc_acc = []

        if default_input:
            vdoc_acc += [
                [{f: item[f] for f in default_input} for item in fields_extracted_raw]
            ]

        if conf.vcol_map(vcol):
            vdoc_acc += [
                [
                    {v: item[k] for k, v in conf.vcol_map(vcol).items()}
                    for item in fields_extracted_raw
                ]
            ]

        for (f_input, f_output), rows_transformed in transformed_fields.items():
            if set(f_output) & set(current_fields):
                vdoc_acc += [rows_transformed]

        vdocs[vcol] = [dict(ChainMap(*auxs)) for auxs in zip(*vdoc_acc)]

    for wdef in conf.current_weights:
        for edges_def in wdef["edge_collections"]:
            u, v = edges_def["source"]["name"], edges_def["target"]["name"]
            acc = []
            for f in edges_def["fields"]:
                for (f_input, f_output), rows_transformed in transformed_fields.items():
                    if {f} & set(f_output):
                        acc += [rows_transformed]
                if f in header_dict.keys():
                    acc += [
                        [
                            {f: item[f]} if f in item else {}
                            for item in fields_extracted_raw
                        ]
                    ]
            weights[(u, v)] = [dict(ChainMap(*auxs)) for auxs in zip(*acc)]

    # if blank collection has no aux fields - inflate it
    for vcol in conf.blank_collections:
        if not vdocs[vcol]:
            vdocs[vcol] = [{}] * len(rows)

    for g in conf.current_graphs:
        if conf.graphs_def[g]["type"] == "direct":
            u, v = g
            if u not in conf.blank_collections and v not in conf.blank_collections:
                if g in weights:
                    edocs[g] = [
                        {"source": x, "target": y, "attributes": attr}
                        for x, y, attr in zip(vdocs[u], vdocs[v], weights[g])
                    ]
                else:
                    edocs[g] = [
                        {"source": x, "target": y} for x, y in zip(vdocs[u], vdocs[v])
                    ]

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
            for vcol, data in vdocuments.items():
                # blank nodes: push and get back their keys  {"_key": ...}
                if vcol in conf.blank_collections:
                    query0 = insert_return_batch(data, conf.vmap[vcol])
                    cursor = db_client.aql.execute(query0)
                    vdocuments[vcol] = [item for item in cursor]
                else:
                    query0 = upsert_docs_batch(
                        data, conf.vmap[vcol], conf.index_fields_dict[vcol], "doc", True
                    )
                    cursor = db_client.aql.execute(query0)

            # update edge data with blank node edges
            for vcol in conf.blank_collections:
                for vfrom, vto in conf.current_graphs:
                    if vcol == vfrom or vcol == vto:
                        edocuments[(vfrom, vto)] = [
                            {"source": x, "target": y}
                            for x, y in zip(vdocuments[vfrom], vdocuments[vto])
                        ]

            for (vfrom, vto), data in edocuments.items():
                query0 = insert_edges_batch(
                    data,
                    conf.vmap[vfrom],
                    conf.vmap[vto],
                    conf.graphs_def[vfrom, vto]["edge_name"],
                    conf.index_fields_dict[vfrom],
                    conf.index_fields_dict[vto],
                    False,
                )
                cursor = db_client.aql.execute(query0)


def prepare_config(config):

    conf_obj = Configurator()
    parse_vcollection(config, conf_obj)

    # vertex_collection -> (table field -> collection field)
    conf_obj.vcollection_fmaps_map = parse_input_output_field_map(config["csv"])

    conf_obj.transformation_maps = parse_transformations(config["csv"])

    conf_obj.encodings = parse_encodings(config["csv"])

    conf_obj.logic = parse_logic(config["csv"])

    parse_graph(config, conf_obj)

    parse_modes2graphs(config["csv"], conf_obj)

    parse_weights(config["csv"], conf_obj)

    return conf_obj
