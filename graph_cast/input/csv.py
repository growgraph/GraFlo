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
        transform_maps[item["filetype"]] = {
            vc["type"]: vc["transforms"]
            for vc in item["vertex_collections"]
            if "transforms" in vc
        }
    return transform_maps


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
    transforms,
):

    vdocs = {}
    edocs = {}
    weights = {}

    for vcol in current_collections:
        vcol_map = field_maps[vcol] if vcol in field_maps else dict()

        rename_input = set(vcol_map.keys())
        default_input = set(vcollection_fields_map[vcol]) - set(vcol_map.values())
        if vcol in transforms:
            t_input = set([item for t in transforms[vcol] for item in t["input"]])
            t_output = set([item for t in transforms[vcol] for item in t["output"]])
            assert not t_input.intersection(vcol_map.keys())
            default_input = default_input - t_output
            input_fields = default_input | rename_input | t_input
        else:
            input_fields = default_input | rename_input

        vdocs_extracted = [
            {f: item[header_dict[f]] for f in input_fields} for item in rows
        ]

        # use map
        vdocs_extracted = [
            {(vcol_map[k] if k in vcol_map else k): v for k, v in doc.items()}
            for doc in vdocs_extracted
        ]

        # use transforms

        if vcol in transforms:
            for doc_ in vdocs_extracted:
                for t in transforms[vcol]:
                    doc_.update(transform_foo(t, doc_))

        # project
        vdocs_extracted = [
            {k: v for k, v in doc.items() if k in vertex_collection_fields[vcol]}
            for doc in vdocs_extracted
        ]

        vdocs[vcol] = add_none_flag(vdocs_extracted, index_fields_dict[vcol])

    for wdef in weights_definition:
        wmap = wdef["map_fields"]
        weights_extracted = [
            {v: item[header_dict[k]] for k, v in wmap.items()} for item in rows
        ]
        acc = []
        if "transforms" in wdef:
            for transformation in wdef["transforms"]:
                acc.append(
                    [transform_foo(transformation, doc) for doc in weights_extracted]
                )
            weights_extracted = [dict(ChainMap(*auxs)) for auxs in zip(*acc)]

        for edges_def in wdef["edges"]:
            u, v = edges_def["source"]["name"], edges_def["target"]["name"]
            weights[(u, v)] = weights_extracted

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
    transforms,
    db_client,
):
    chk = Chunker(fname, batch_size, max_lines)
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
                transforms,
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
