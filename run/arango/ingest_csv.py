import time
import argparse
import yaml
from os import listdir
from os.path import isfile, join
import csv
from itertools import permutations
import logging
from graph_cast.arango.util import (
    delete_collections,
    upsert_docs_batch,
    insert_edges_batch,
    define_extra_edges,
    update_to_numeric,
)
from graph_cast.util.transform import clear_first_level_nones
from graph_cast.arango.util import get_arangodb_client
from graph_cast.util.io import Chunker

logger = logging.getLogger(__name__)


def main(
    fpath,
    db_client,
    limit_files=None,
    max_lines=None,
    batch_size=50000000,
    modes=("publications", "contributors", "institutions", "refs"),
    clean_start="all",
    config=None,
):

    # vertex_type -> vertex_collection_name
    vmap = {
        k: f'{v["basename"]}' for k, v in config["vertex_collections"].items()
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
    for item in config["csv"]:
        field_maps[item["filetype"]] = {
            vmap[vc["type"]]: vc["map_fields"]
            for vc in item["vertex_collections"]
            if "map_fields" in vc
        }

    acc = []
    for n in config["csv"]:
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

    logger.info(graph)

    files_dict = {}

    for keyword in modes:
        files_dict[keyword] = sorted(
            [f for f in listdir(fpath) if isfile(join(fpath, f)) and keyword in f]
        )

    if limit_files:
        files_dict = {k: v[:limit_files] for k, v in files_dict.items()}

    logger.info(files_dict)

    logger.info(f"clean start {clean_start}")
    if clean_start == "all":
        delete_collections(db_client, vcollections + ecollections, actual_graphs)
    elif clean_start == "edges":
        delete_collections(db_client, ecollections, [])

    if clean_start == "edges":
        for gname in actual_graphs:
            vcol_from, vcol_to, edge_col = (
                graph[gname]["source"],
                graph[gname]["target"],
                graph[gname]["edge_name"],
            )
            logger.info(f"{vcol_from}, {vcol_to}, {edge_col}")
            if db_client.has_graph(gname):
                g = db_client.graph(gname)
            else:
                g = db_client.create_graph(gname)
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
            logger.info(f"{vcol_from}, {vcol_to}, {edge_col}")
            if db_client.has_graph(gname):
                g = db_client.graph(gname)
            else:
                g = db_client.create_graph(gname)
            if not db_client.has_collection(vcol_to):
                _ = g.create_vertex_collection(vcol_to)
                general_collection = db_client.collection(vcol_to)
                index_fields = index_fields_dict[vcol_to]
                ih = general_collection.add_hash_index(fields=index_fields, unique=True)
            if not db_client.has_collection(vcol_from):
                _ = g.create_vertex_collection(vcol_from)
                general_collection = db_client.collection(vcol_from)
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
                general_collection = db_client.collection(cname)
                ih = general_collection.add_hash_index(
                    fields=index_dict["fields"], unique=index_dict["unique"]
                )

    logger.info([c["name"] for c in db_client.collections() if c["name"][0] != "_"])
    seconds_start0 = time.time()

    for mode in modes:
        seconds_start_mode = time.time()

        for filename in files_dict[mode]:
            seconds_start_file = time.time()
            chk = Chunker(join(fpath, filename), batch_size, max_lines)
            header = chk.pop_header()
            header = header.split(",")
            header_dict = dict(zip(header, range(len(header))))
            logger.info(f"header_dict {header_dict}")

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
                        logger.info(f"vfrom_dict {vfrom_dict}")

                        vfrom_header_dict = {
                            (vfrom_dict[k] if k in vfrom_dict else k): v
                            for k, v in header_dict.items()
                        }

                        logger.info(f"vfrom_header_dict {vfrom_header_dict}")

                        retrieve_fields_dict_from = [
                            f
                            for f in retrieve_fields_dict[vfrom]
                            if f in vfrom_header_dict
                        ]
                        logger.info(f"retrieve_fields_dict_from {retrieve_fields_dict_from}")

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
                        cursor = db_client.aql.execute(query0)

                        logger.info(f"vto_dict {vto_dict}")

                        vto_header_dict = {
                            (vto_dict[k] if k in vto_dict else k): v
                            for k, v in header_dict.items()
                        }

                        logger.info(f"vto_header_dict {vto_header_dict}")

                        retrieve_fields_dict_to = [
                            f for f in retrieve_fields_dict[vto] if f in vto_header_dict
                        ]

                        logger.info(f"retrieve_fields_dict_to {retrieve_fields_dict_to}")

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
                        # logger.info(query0)
                        cursor = db_client.aql.execute(query0)

                        seconds2 = time.time()
                        logger.info(
                            f"ingested {len(from_set) + len(to_set)} nodes; {seconds2 - seconds0:.1f} sec"
                        )

                        edges_ = [
                            {"source": x, "target": y}
                            for x, y in zip(from_list, to_list)
                        ]
                        logger.info(index_fields_dict[vfrom])
                        logger.info(index_fields_dict[vto])

                        query0 = insert_edges_batch(
                            edges_,
                            vfrom,
                            vto,
                            ecol,
                            index_fields_dict[vfrom],
                            index_fields_dict[vto],
                            False,
                        )
                        cursor = db_client.aql.execute(query0)

                        seconds3 = time.time()
                        logger.info(
                            f"ingested {len(edges_)} edges; {seconds3 - seconds2:.1f} sec"
                        )
            seconds_end_file = time.time()
            logger.info(
                f"ingest file {filename} took {(seconds_end_file - seconds_start_file) :.1f} sec"
            )
        seconds_end_mode = time.time()
        logger.info(
            f"ingest mode {mode} took {(seconds_end_mode - seconds_start_mode) :.1f} sec"
        )
    seconds_end0 = time.time()
    logger.info(f"full ingest took {(seconds_end0 - seconds_start0) :.1f} sec")

    logger.info(f"updating some fields to numeric...")
    seconds_start0 = time.time()

    for cname, fields in numeric_fields_dict.items():
        for field in fields:
            query0 = update_to_numeric(cname, field)
            cursor = db_client.aql.execute(query0)
    seconds_end0 = time.time()
    logger.info(f"updating some fields to numeric {(seconds_end0 - seconds_start0) :.1f} sec")

    logger.info(f"defining edges for extra graphs...")
    seconds_start0 = time.time()

    # create edge u -> v from u->w, v->w edges
    # find edge_cols uw and vw
    for gname, item in graph.items():
        if item["type"] == "indirect":
            query0 = define_extra_edges(item)
            cursor = db_client.aql.execute(query0)
    seconds_end0 = time.time()
    logger.info(f"defined edges for extra graphs {(seconds_end0 - seconds_start0) :.4f} sec")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    logging.basicConfig(
        filename="ingest_csv.log",
        format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )

    parser.add_argument("--path", type=str, help="path to csv datafiles")

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
        "-l", "--cred-name", default="root", help="login name for arangodb connection"
    )

    parser.add_argument(
        "-w",
        "--cred-pass",
        default="123",
        help="login password for arangodb connection",
    )

    parser.add_argument("--db",
                        # default="_system",
                        default="wos",
                        help="db for arangodb connection")

    parser.add_argument(
        "-f",
        "--limit-files",
        default=None,
        type=int,
        nargs="?",
        help="max files per type to use for ingestion",
    )

    parser.add_argument(
        "-m",
        "--max-lines",
        default=None,
        type=int,
        nargs="?",
        help="max lines per file to use for ingestion",
    )

    parser.add_argument(
        "-b",
        "--batch-size",
        default=500000,
        type=int,
        help="number of symbols read from (archived) file for a single batch",
    )

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
        default="../conf/wos.yaml",
        help="",
    )

    args = parser.parse_args()

    with open(args.config_path, "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)

    logging.basicConfig(filename="ingest_csv.log", level=logging.INFO)

    db_client = get_arangodb_client(
        args.protocol, args.id_addr, args.port, args.db, args.cred_name, args.cred_pass
    )

    main(
        args.path,
        db_client,
        args.limit_files,
        args.max_lines,
        args.batch_size,
        args.modes,
        args.clean_start,
        config,
    )
