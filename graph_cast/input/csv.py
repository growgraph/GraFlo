import csv
from collections import defaultdict
from itertools import permutations
from os import listdir
from os.path import isfile, join

from graph_cast.util.io import Chunker
from graph_cast.util.transform import add_none_flag

from graph_cast.arango.util import upsert_docs_batch, insert_edges_batch


def parse_edges(config):
    edges = config["edge_collections"].copy()
    extra_edges = config["extra_edges"].copy()
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
    field_maps,
    index_fields_dict,
    vcollection_fields_map,
    graph,
):

    vdocs = {}
    edocs = {}
    for vcol in current_collections:
        vcol_map = field_maps[vcol] if vcol in field_maps else dict()

        vfrom_header_dict = {
            (vcol_map[k] if k in vcol_map else k): v for k, v in header_dict.items()
        }
        retrieve_fields_dict_from = [
            f for f in vcollection_fields_map[vcol] if f in vfrom_header_dict
        ]

        from_list = [
            {f: item[vfrom_header_dict[f]] for f in retrieve_fields_dict_from}
            for item in rows
        ]

        vdocs[vcol] = add_none_flag(from_list, index_fields_dict[vcol])

    for g in current_graphs:
        if graph[g]["type"] != "direct":
            pass
        u, v = g
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
    graph,
    field_maps,
    index_fields_dict,
    vmap,
    vcollection_fields_map,
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
                field_maps,
                index_fields_dict,
                vcollection_fields_map,
                graph,
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
                    graph[vfrom, vto]["edge_name"],
                    index_fields_dict[vfrom],
                    index_fields_dict[vto],
                    False,
                )
                cursor = db_client.aql.execute(query0)
