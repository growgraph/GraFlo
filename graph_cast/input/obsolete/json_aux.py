import gzip
import json
import logging
from collections import ChainMap, defaultdict

from graph_cast.util.chunking import FPSmart

logger = logging.getLogger(__name__)


xml_dummy = "#text"


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
                edge_acc_, mapping_fields = parse_edges(
                    m, edge_acc, mapping_fields
                )
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
            if (
                "wos_standard" in item
                and item[discriminant_key] == discriminant_value
            ):
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
