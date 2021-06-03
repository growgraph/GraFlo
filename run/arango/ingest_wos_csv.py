import time
import argparse
import yaml
from os.path import join, expanduser
from os import listdir
from os.path import isfile, join
import csv
from itertools import permutations
from arango import ArangoClient
from graph_cast.util.db import (
    delete_collections,
    upsert_docs_batch,
    insert_edges_batch,
    define_extra_edges,
)
from graph_cast.utils import clear_first_level_nones, update_to_numeric
from graph_cast.chunker import Chunker
from pprint import pprint


def is_int(x):
    try:
        int(x)
    except:
        return False
    return True


def main(
    fpath,
    protocol="http",
    ip_addr="127.0.0.1",
    port=8529,
    database="_system",
    cred_name="root",
    cred_pass="123",
    limit_files=None,
    max_lines=None,
    batch_size=50000000,
    modes=("publications", "contributors", "institutions", "refs"),
    clean_start="all",
    prefix="toy_",
    config=None,
    verbose=True,
):
    # vertex_type -> vertex_collection_name
    vmap = {
        k: f'{prefix}{v["basename"]}' for k, v in config["vertex_collections"].items()
    }

    # vertex_collection_name -> field_definition
    index_fields_dict = {
        vmap[k]: v["index"] for k, v in config["vertex_collections"].items()
    }

    # vertex_collection_name -> extra_index
    # in addition to index from field_definition
    extra_indices = {
        vmap[k]: v["extra_index"]
        for k, v in config["vertex_collections"].items()
        if "extra_index" in v
    }

    # vertex_collection_name -> fields_keep
    retrieve_fields_dict = {
        vmap[k]: v["fields"] for k, v in config["vertex_collections"].items()
    }

    # vertex_collection_name -> fields_type
    numeric_fields_dict = {
        vmap[k]: v["numeric_fields"]
        for k, v in config["vertex_collections"].items()
        if "numeric_fields" in v
    }

    #############################
    # edge discovery

    field_maps = {}
    for item in config["table"]:
        field_maps[item["filetype"]] = {
            vmap[vc["type"]]: vc["map_fields"]
            for vc in item["vertex_collections"]
            if "map_fields" in vc
        }

    acc = []
    for n in config["table"]:
        for pa, pb in permutations(n["vertex_collections"], 2):
            pa_map = pa["map_fields"] if "map_fields" in pa else {}
            pb_map = pb["map_fields"] if "map_fields" in pb else {}
            item = (pa["type"], pb["type"], pa_map, pb_map, n["filetype"])
            acc += [item]

    # [(vcol_a, vcol_b, map_table_vcol_a, map_table_vcol_b, table)]
    edges_def = []
    requested_edge_collections = config["edge_collections"].copy()
    for item in acc:
        a, b, _, _, _ = item
        if [a, b] in requested_edge_collections:
            edges_def += [(vmap[a], vmap[b], *item[2:])]
            requested_edge_collections.remove([a, b])
    edges_def = sorted(edges_def)

    extra_edges = config["extra_edges"]

    graph = {}
    modes2graphs = {}
    for uv in edges_def:
        u, v, udict, vdict, table_source = uv
        graph_name = f"{u}_{v}_graph"
        ecollection_name = f"{u}_{v}_edges"

        graph[graph_name] = {
            "source": u,
            "target": v,
            "edge_name": ecollection_name,
            "source_map": udict,
            "target_map": vdict,
            "type": "direct",
        }

        if table_source in modes2graphs:
            modes2graphs[table_source] += [graph_name]
        else:
            modes2graphs[table_source] = [graph_name]

    for item in extra_edges:
        u, v = vmap[item["source"]], vmap[item["target"]]
        graph_name = f"{u}_{v}_graph"
        ecollection_name = f"{u}_{v}_edges"
        graph[graph_name] = {
            "source": u,
            "target": v,
            "edge_name": ecollection_name,
            "by": vmap[item["by"]],
            "edge_weight": item["edge_weight"],
            "type": "indirect",
        }

    actual_graphs = [g for item in modes2graphs.values() for g in item]

    vcollections = list(
        set([graph[g]["source"] for g in actual_graphs])
        | set([graph[g]["target"] for g in actual_graphs])
    )

    ecollections = list(set([graph[g]["edge_name"] for g in actual_graphs]))

    if verbose:
        pprint(graph)

    files_dict = {}

    for keyword in modes:
        files_dict[keyword] = sorted(
            [f for f in listdir(fpath) if isfile(join(fpath, f)) and keyword in f]
        )

    if limit_files:
        files_dict = {k: v[:limit_files] for k, v in files_dict.items()}

    pprint(files_dict)

    hosts = f"{protocol}://{ip_addr}:{port}"
    client = ArangoClient(hosts=hosts)

    sys_db = client.db(database, username=cred_name, password=cred_pass)

    if verbose:
        print(f"clean start {clean_start}")
    if clean_start == "all":
        delete_collections(sys_db, vcollections + ecollections, actual_graphs)
    elif clean_start == "edges":
        delete_collections(sys_db, ecollections, [])

    if clean_start == "edges":
        for gname in actual_graphs:
            vcol_from, vcol_to, edge_col = (
                graph[gname]["source"],
                graph[gname]["target"],
                graph[gname]["edge_name"],
            )
            if verbose:
                print("********************")
                print(vcol_from, vcol_to, edge_col)
            if sys_db.has_graph(gname):
                g = sys_db.graph(gname)
            else:
                g = sys_db.create_graph(gname)
            _ = g.create_edge_definition(
                edge_collection=edge_col,
                from_vertex_collections=[vcol_from],
                to_vertex_collections=[vcol_to],
            )

    if clean_start == "all":
        for gname in actual_graphs:
            vcol_from, vcol_to, edge_col = (
                graph[gname]["source"],
                graph[gname]["target"],
                graph[gname]["edge_name"],
            )
            if verbose:
                print("********************")
                print(vcol_from, vcol_to, edge_col)
            if sys_db.has_graph(gname):
                g = sys_db.graph(gname)
            else:
                g = sys_db.create_graph(gname)
            if not sys_db.has_collection(vcol_to):
                _ = g.create_vertex_collection(vcol_to)
                general_collection = sys_db.collection(vcol_to)
                index_fields = index_fields_dict[vcol_to]
                ih = general_collection.add_hash_index(fields=index_fields, unique=True)
            if not sys_db.has_collection(vcol_from):
                _ = g.create_vertex_collection(vcol_from)
                general_collection = sys_db.collection(vcol_from)
                index_fields = index_fields_dict[vcol_from]
                ih = general_collection.add_hash_index(fields=index_fields, unique=True)

            _ = g.create_edge_definition(
                edge_collection=edge_col,
                from_vertex_collections=[vcol_from],
                to_vertex_collections=[vcol_to],
            )

        # add secondary indices:
        for cname, list_indices in extra_indices.items():
            for index_dict in list_indices:
                general_collection = sys_db.collection(cname)
                ih = general_collection.add_hash_index(
                    fields=index_dict["fields"], unique=index_dict["unique"]
                )

    print([c["name"] for c in sys_db.collections() if c["name"][0] != "_"])
    seconds_start0 = time.time()

    for mode in modes:
        seconds_start_mode = time.time()

        for filename in files_dict[mode]:
            seconds_start_file = time.time()
            chk = Chunker(join(fpath, filename), batch_size, max_lines)
            header = chk.pop_header()
            header = header.split(",")
            header_dict = dict(zip(header, range(len(header))))
            if verbose:
                print("header_dict")
                print(header_dict)

            seconds_start = time.time()

            while not chk.done:
                lines = chk.pop()
                if lines:
                    lines2 = [
                        next(csv.reader([line.rstrip()], skipinitialspace=True))
                        for line in lines
                    ]

                    for g in modes2graphs[mode]:
                        if graph[g]["type"] != "direct":
                            pass

                        vfrom, vto, ecol = (
                            graph[g]["source"],
                            graph[g]["target"],
                            graph[g]["edge_name"],
                        )
                        vfrom_dict, vto_dict = (
                            graph[g]["source_map"],
                            graph[g]["target_map"],
                        )

                        seconds0 = time.time()
                        if verbose:
                            print("vfrom_dict")
                            print(vfrom_dict)

                        vfrom_header_dict = {
                            (vfrom_dict[k] if k in vfrom_dict else k): v
                            for k, v in header_dict.items()
                        }

                        if verbose:
                            print("vfrom_header_dict")
                            print(vfrom_header_dict)

                        retrieve_fields_dict_from = [
                            f
                            for f in retrieve_fields_dict[vfrom]
                            if f in vfrom_header_dict
                        ]
                        if verbose:
                            print("retrieve_fields_dict_from")
                            print(retrieve_fields_dict_from)

                        from_list = [
                            {
                                f: item[vfrom_header_dict[f]]
                                for f in retrieve_fields_dict_from
                            }
                            for item in lines2
                        ]

                        from_list = clear_first_level_nones(
                            from_list, index_fields_dict[vfrom]
                        )

                        # unique on index_fields_dict[vfrom]
                        from_set = [
                            dict(y) for y in set(tuple(x.items()) for x in from_list)
                        ]
                        query0 = upsert_docs_batch(
                            from_set, vfrom, index_fields_dict[vfrom], "doc", True
                        )
                        cursor = sys_db.aql.execute(query0)

                        if verbose:
                            print("vto_dict")
                            print(vto_dict)

                        vto_header_dict = {
                            (vto_dict[k] if k in vto_dict else k): v
                            for k, v in header_dict.items()
                        }

                        if verbose:
                            print("vto_header_dict")
                            print(vto_header_dict)

                        retrieve_fields_dict_to = [
                            f for f in retrieve_fields_dict[vto] if f in vto_header_dict
                        ]
                        if verbose:
                            print("retrieve_fields_dict_to")
                            print(retrieve_fields_dict_to)

                        to_list = [
                            {
                                f: item[vto_header_dict[f]]
                                for f in retrieve_fields_dict_to
                            }
                            for item in lines2
                        ]

                        to_list = clear_first_level_nones(
                            to_list, index_fields_dict[vto]
                        )
                        # unique on index_fields_dict[vfrom]
                        to_set = [
                            dict(y) for y in set(tuple(x.items()) for x in to_list)
                        ]

                        query0 = upsert_docs_batch(
                            to_set, vto, index_fields_dict[vto], "doc", True
                        )
                        # print(query0)
                        cursor = sys_db.aql.execute(query0)

                        seconds2 = time.time()
                        if verbose:
                            print(
                                f"ingested {len(from_set) + len(to_set)} nodes; {seconds2 - seconds0:.1f} sec"
                            )

                        edges_ = [
                            {"source": x, "target": y}
                            for x, y in zip(from_list, to_list)
                        ]
                        if verbose:
                            print(index_fields_dict[vfrom])
                            print(index_fields_dict[vto])
                        query0 = insert_edges_batch(
                            edges_,
                            vfrom,
                            vto,
                            ecol,
                            index_fields_dict[vfrom],
                            index_fields_dict[vto],
                            False,
                        )
                        cursor = sys_db.aql.execute(query0)

                        seconds3 = time.time()
                        if verbose:
                            print(
                                f"ingested {len(edges_)} edges; {seconds3 - seconds2:.1f} sec"
                            )
            seconds_end_file = time.time()
            print(
                f"ingest file {filename} took {(seconds_end_file - seconds_start_file) :.1f} sec"
            )
        seconds_end_mode = time.time()
        print(
            f"ingest mode {mode} took {(seconds_end_mode - seconds_start_mode) :.1f} sec"
        )
    seconds_end0 = time.time()
    print(f"full ingest took {(seconds_end0 - seconds_start0) :.1f} sec")

    print(f"updating some fields to numeric...")
    seconds_start0 = time.time()

    for cname, fields in numeric_fields_dict.items():
        for field in fields:
            query0 = update_to_numeric(cname, field)
            cursor = sys_db.aql.execute(query0)
    seconds_end0 = time.time()
    print(f"updating some fields to numeric {(seconds_end0 - seconds_start0) :.1f} sec")

    print(f"defining edges for extra graphs...")
    seconds_start0 = time.time()

    # create edge u -> v from u->w, v->w edges
    # find edge_cols uw and vw
    for gname, item in graph.items():
        if item["type"] == "indirect":
            query0 = define_extra_edges(item)
            cursor = sys_db.aql.execute(query0)
    seconds_end0 = time.time()
    print(f"defined edges for extra graphs {(seconds_end0 - seconds_start0) :.4f} sec")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d", "--datapath", default=expanduser("../data/toy"), help="Path to data files"
    )

    parser.add_argument(
        "-i",
        "--id-addr",
        default="127.0.0.1",
        type=str,
        help="port for arangodb connection",
    )

    parser.add_argument(
        "--protocol", default="http", type=str, help="protocol for arangodb connection"
    )

    parser.add_argument(
        "-p", "--port", default=8529, type=int, help="port for arangodb connection"
    )

    parser.add_argument(
        "-l", "--login-name", default="root", help="login name for arangodb connection"
    )

    parser.add_argument(
        "-w",
        "--login-password",
        default="123",
        help="login password for arangodb connection",
    )

    parser.add_argument("--db", default="_system", help="db for arangodb connection")

    parser.add_argument(
        "-f",
        "--limit-files",
        default=None,
        type=str,
        help="max files per type to use for ingestion",
    )

    parser.add_argument(
        "-m",
        "--max-lines",
        default=None,
        type=str,
        help="max lines per file to use for ingestion",
    )

    parser.add_argument(
        "-v", "--verbose", default=False, type=bool, help="verbosity level"
    )

    parser.add_argument(
        "-b",
        "--batch-size",
        default=500000,
        type=int,
        help="number of symbols read from (archived) file for a single batch",
    )

    parser.add_argument("--prefix", default="toy_", help="prefix for collection names")

    parser.add_argument(
        "--modes",
        nargs="*",
        default=["publications", "contributors", "institutions", "refs"],
    )

    parser.add_argument(
        "--clean-start",
        type=str,
        default="all",
        help='"all" to wipe all the collections, "edges" to wipe only edges',
    )

    parser.add_argument(
        "--config-path",
        type=str,
        default="../../conf/wos.yaml",
        help="",
    )

    args = parser.parse_args()

    if is_int(args.limit_files):
        limit_files = int(args.limit_files)
    else:
        limit_files = None

    if is_int(args.max_lines):
        max_lines = int(args.max_lines)
    else:
        max_lines = None

    fpath = args.datapath

    id_addr = args.id_addr
    protocol = args.protocol
    port = args.port
    database = args.db
    cred_name = args.login_name
    cred_pass = args.login_password

    verbose = args.verbose
    batch_size = args.batch_size
    modes = args.modes
    clean_start = args.clean_start

    prefix = args.prefix

    with open(args.config_path, "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    if verbose:
        print(f"max_lines : {max_lines}; limit_files: {limit_files}")
        print(f"modes: {modes}")
        print(f"clean start: {clean_start}")

    main(
        fpath,
        protocol,
        id_addr,
        port,
        database,
        cred_name,
        cred_pass,
        limit_files,
        max_lines,
        batch_size,
        modes,
        clean_start,
        prefix,
        config,
        verbose,
    )
