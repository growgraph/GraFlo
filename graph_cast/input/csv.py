import csv
from itertools import permutations
from collections import defaultdict, ChainMap
from os import listdir
from os.path import isfile, join

from graph_cast.util.io import Chunker
from graph_cast.util.transform import add_none_flag

from graph_cast.arango.util import upsert_docs_batch, insert_edges_batch
from graph_cast.input.util import transform_foo


def parse_edges(config):
    edges = config["edge_collections"].copy()
    if "extra_edges" in config:
        extra_edges = config["extra_edges"].copy()
    else:
        extra_edges = {}
    return edges, extra_edges


def parse_input_output_field_map(subconfig):
    field_maps = {}
    for item in subconfig:
        field_maps[item["filetype"]] = {
            vc["type"]: vc["map_fields"]
            for vc in item["vertex_collections"]
            if "map_fields" in vc
        }
    return field_maps


def parse_transformations(subconfig):
    transform_maps = {}
    for item in subconfig:
        if "transforms" in item:
            transform_maps[item["filetype"]] = item["transforms"]
    return transform_maps


def parse_encodings(subconfig):
    encodings_map = {}
    for item in subconfig:
        encodings_map[item["filetype"]] = item["encoding"]
    return encodings_map


def derive_modes2graphs(graph, subconfig):
    modes2graphs = defaultdict(list)
    modes2collections = dict()
    direct_graph = {k: v for k, v in graph.items() if v["type"] == "direct"}

    for item in subconfig:
        ftype = item["filetype"]

        vcols = [iitem["type"] for iitem in item["vertex_collections"]]
        modes2collections[ftype] = list(set(vcols))
        for u, v in permutations(vcols, 2):
            if (u, v) in direct_graph:
                modes2graphs[ftype] += [(u, v)]

    modes2graphs = {k: list(set(v)) for k, v in modes2graphs.items()}

    return modes2graphs, modes2collections


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
    current_collections,
    current_graphs,
    header_dict,
    vertex_collection_fields,
    field_maps,
    index_fields_dict,
    vcollection_fields_map,
    graphs_definition,
    weights_definition,
    current_transformations,
):

    vdocs = {}
    edocs = {}
    weights = {}

    # do possible transforms

    fields_extracted_raw = [
        {k: item[v] for k, v in header_dict.items()} for item in rows
    ]

    transformed_fields = {}

    for transformation in current_transformations:
        inputs = transformation["input"]
        outputs = transformation["output"]
        transformed_fields[(tuple(inputs), tuple(outputs))] = [transform_foo(transformation, doc)
                                                               for doc in fields_extracted_raw]

    for vcol in current_collections:
        vcol_map = field_maps[vcol] if vcol in field_maps else dict()
        current_fields = vcollection_fields_map[vcol]
        default_input = set(current_fields) & set(header_dict.keys())
        vdoc_acc = []

        for (f_input, f_output), rows_transformed in transformed_fields.items():
            if set(f_output) & set(current_fields):
                  vdoc_acc += [rows_transformed]

        if vcol_map:
            vdoc_acc += [[
                {v: item[k] for k, v in vcol_map.items()} for item in fields_extracted_raw
            ]]
        if default_input:
            vdoc_acc += [[
                {f: item[f] for f in default_input} for item in fields_extracted_raw
            ]]

        vdocs[vcol] = [dict(ChainMap(*auxs)) for auxs in zip(*vdoc_acc)]

        vdocs[vcol] = add_none_flag(vdocs[vcol], index_fields_dict[vcol])

    for wdef in weights_definition:
        for edges_def in wdef["edge_collections"]:
            u, v = edges_def["source"]["name"], edges_def["target"]["name"]
            acc = []
            for f in edges_def["fields"]:
                for (f_input, f_output), rows_transformed in transformed_fields.items():
                    if {f} & set(f_output):
                        acc += [rows_transformed]
            weights[(u, v)] = [dict(ChainMap(*auxs)) for auxs in zip(*acc)]

    for g in current_graphs:
        if graphs_definition[g]["type"] != "direct":
            pass
        u, v = g
        if g in weights:
            edocs[g] = [
                {"source": x, "target": y, "attributes": attr}
                for x, y, attr in zip(vdocs[u], vdocs[v], weights[g])
                if "_flag_na" not in x and "_flag_na" not in y
            ]
        else:
            edocs[g] = [
                {"source": x, "target": y}
                for x, y in zip(vdocs[u], vdocs[v])
                if "_flag_na" not in x and "_flag_na" not in y
            ]

    return vdocs, edocs


def process_csv(
    fname,
    batch_size,
    max_lines,
    current_graphs,
    current_collections,
    graphs_definition,
    vertex_collection_fields,
    field_maps,
    index_fields_dict,
    vmap,
    vcollection_fields_map,
    weights_definition,
    current_transformations,
    db_client,
    encoding
):
    chk = Chunker(fname, batch_size, max_lines, encoding=encoding)
    header = chk.pop_header()
    header = header.split(",")
    header_dict = dict(zip(header, range(len(header))))

    while not chk.done:
        lines = chk.pop()
        if lines:
            lines2 = [
                next(csv.reader([line.rstrip()], skipinitialspace=True))
                for line in lines
            ]

            vdocuments, edocuments = table_to_vcollections(
                lines2,
                current_collections,
                current_graphs,
                header_dict,
                vertex_collection_fields,
                field_maps,
                index_fields_dict,
                vcollection_fields_map,
                graphs_definition,
                weights_definition,
                current_transformations,
            )

            # TODO move db related stuff out
            for vcol, data in vdocuments.items():
                query0 = upsert_docs_batch(
                    data, vmap[vcol], index_fields_dict[vcol], "doc", True
                )
                cursor = db_client.aql.execute(query0)

            for (vfrom, vto), data in edocuments.items():
                query0 = insert_edges_batch(
                    data,
                    vmap[vfrom],
                    vmap[vto],
                    graphs_definition[vfrom, vto]["edge_name"],
                    index_fields_dict[vfrom],
                    index_fields_dict[vto],
                    False,
                )
                cursor = db_client.aql.execute(query0)
