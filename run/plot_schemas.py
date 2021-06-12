import yaml
import os
import networkx as nx
from collections import defaultdict
from itertools import product
import argparse

from graph_cast.input.json import parse_edges

"""

graphviz attributes 

https://renenyffenegger.ch/notes/tools/Graphviz/attributes/index
https://rsms.me/graphviz/
https://graphviz.readthedocs.io/en/stable/examples.html
https://graphviz.org/doc/info/attrs.html

usage: 
    color='red',style='filled', fillcolor='blue',shape='square'

to keep 
level_one = [node1, node2]
sg_one = ag.add_subgraph(level_one, rank='same')

"""

fillcolor_palette = {
    "violet": "#DDD0E5",
    "green": "#BEDFC8",
    "blue": "#B7D1DF",
    "red": "#EBA59E",
}

map_type2shape = {
    "table": "box",
    "vcollection": "ellipse",
    "index": "polygon",
    "field": "octagon",
    "blank": "box",
    "def_field": "trapezium",
}

map_type2color = {
    "table": fillcolor_palette["blue"],
    "vcollection": fillcolor_palette["green"],
    "index": "orange",
    "def_field": fillcolor_palette["red"],
    "field": fillcolor_palette["violet"],
    "blank": "white",
}

edge_status = {"vcollection": "dashed", "table": "solid"}


def parse_branch(croot, acc, nc):
    """
    extract edge definition and edge fields from definition dict
    :param croot:
    :param acc:
    :param nc:
    :return:
    """
    if isinstance(croot, dict):
        if "maps" in croot:
            if "descend_key" in croot:
                nleft = (croot["descend_key"], "blank")
            else:
                nleft = nc
            for m in croot["maps"]:
                acc, cnode = parse_branch(m, acc, nleft)
                if nleft != cnode:
                    acc += [(nleft, cnode)]
            return acc, nleft
        elif "name" in croot:
            nleft = (croot["name"], "vcollection")
            return acc, nleft
        else:
            return acc, [(None, "blank")]


class SchemaPlotter:
    def __init__(self, config_filename):
        self.figgpath = "./figs/schema"
        with open(config_filename, "r") as f:
            self.config = yaml.load(f, Loader=yaml.FullLoader)

        self.name = self.config["general"]["name"]

        if "json" in self.config:
            self.type = "json"
        elif "csv" in self.config:
            self.type = "csv"
        else:
            raise KeyError(f"Configured to plot json or csv mapper schemas")

        self.prefix = f"{self.name}_{self.type}"

    def plot_vc2fields(self):
        g = nx.DiGraph()
        nodes = []
        edges = []
        for k, props in self.config["vertex_collections"].items():
            index_fields = props["index"]
            nodes_collection = [(k, {"type": "vcollection"})]
            nodes_fields = [
                (
                    f"{k}:{item}",
                    {
                        "type": "def_field" if item in index_fields else "field",
                        "label": item,
                    },
                )
                for item in props["fields"]
            ]
            nodes += nodes_collection
            nodes += nodes_fields
            edges += [(x[0], y[0]) for x, y in product(nodes_collection, nodes_fields)]

        g.add_nodes_from(nodes)
        g.add_edges_from(edges)

        for n in g.nodes():
            props = g.nodes()[n]
            upd_dict = props.copy()
            if "type" in upd_dict:
                upd_dict["shape"] = map_type2shape[props["type"]]
                upd_dict["color"] = map_type2color[props["type"]]
            if "label" in upd_dict:
                upd_dict["forcelabel"] = True
            upd_dict["style"] = "filled"

            for k, v in upd_dict.items():
                g.nodes[n][k] = v

        for e in g.edges(data=True):
            s, t, _ = e
            upd_dict = {"style": "solid", "arrowhead": "vee"}
            for k, v in upd_dict.items():
                g.edges[s, t][k] = v

        ag = nx.nx_agraph.to_agraph(g)

        for k, props in self.config["vertex_collections"].items():
            level_index = [f"{k}:{item}" for item in props["index"]]
            index_subgraph = ag.add_subgraph(level_index, name=f"cluster_{k}:def")
            index_subgraph.node_attr["style"] = "filled"
            index_subgraph.node_attr["label"] = "definition"

        ag.draw(
            os.path.join(self.figgpath, f"{self.prefix}_vc2fields.pdf"),
            "pdf",
            prog="dot",
        )

    def plot_source2vc(self):
        """
        draw map of source vertices (nodes of json or csv files) to vertex collections


        """
        nodes = []
        if self.type == "json":
            g = nx.DiGraph()
            acc = []
            edges_, _ = parse_branch(self.config[self.type], acc, None)
            edges = [("_".join(x), "_".join(y)) for x, y in edges_]
            for ee in edges_:
                for n in ee:
                    nodes += [("_".join(n), {"type": n[1], "name": n[0]})]

            for nid, weight in nodes:
                g.add_node(nid, **weight)
        elif self.type == "csv":
            g = nx.MultiDiGraph()
            edges = []
            for n in self.config[self.type]:
                k = n["filetype"]
                nodes_table = [(k, {"type": "table"})]
                nodes_collection = [
                    (item["type"], {"type": "vcollection"})
                    for item in n["vertex_collections"]
                ]
                nodes += nodes_table
                nodes += nodes_collection
                edges += [
                    (nt[0], nc[0]) for nt, nc in product(nodes_table, nodes_collection)
                ]

            g.add_nodes_from(nodes)

        g.add_edges_from(edges)

        for n in g.nodes():
            props = g.nodes()[n]
            upd_dict = {
                "shape": map_type2shape[props["type"]],
                "color": map_type2color[props["type"]],
                "style": "filled",
            }
            if "label" in props:
                upd_dict["forcelabel"] = True
            if "name" in props:
                upd_dict["label"] = props["name"]
            for k, v in upd_dict.items():
                g.nodes[n][k] = v

        ag = nx.nx_agraph.to_agraph(g)
        ag.draw(
            os.path.join(self.figgpath, f"{self.prefix}_source2vc.pdf"),
            "pdf",
            prog="dot",
        )

    def plot_vc2vc(self):
        """
            vc -> vc
        :return:
        """
        g = nx.DiGraph()
        if self.type == "json":
            edge_def, excl_fields = parse_edges(
                self.config[self.type], [], defaultdict(list)
            )
            edges = [x[:2] for x in edge_def]
            nodes = [
                (n, {"type": "vcollection"}) for n in self.config["vertex_collections"]
            ]
        elif self.type == "csv":
            nodes = []
            edges = []
            for n in self.config[self.type]:
                nodes_collection = [
                    (item["type"], {"type": "vcollection"})
                    for item in n["vertex_collections"]
                ]
                nodes += nodes_collection

            edges += [(x, y) for x, y in self.config["edge_collections"]]

        g.add_nodes_from(nodes)
        g.add_edges_from(edges)

        for n in g.nodes():
            props = g.nodes()[n]
            upd_dict = {
                "shape": map_type2shape[props["type"]],
                "color": map_type2color[props["type"]],
                "style": "filled",
            }
            for k, v in upd_dict.items():
                g.nodes[n][k] = v

        for e in g.edges(data=True):
            s, t, _ = e
            target_props = g.nodes[s]
            upd_dict = {"style": edge_status[target_props["type"]], "arrowhead": "vee"}
            for k, v in upd_dict.items():
                g.edges[s, t][k] = v

        ag = nx.nx_agraph.to_agraph(g)
        # ['neato' | 'dot' | 'twopi' | 'circo' | 'fdp' | 'nop']
        ag.draw(
            os.path.join(self.figgpath, f"{self.prefix}_vc2vc.pdf"), "pdf", prog="dot"
        )

    def plot_source2vc_detailed(self):

        """
            source (json vertex or table) -> source fields -> vertex collection fields -> vertex collection

        :return:
        """

        g = nx.DiGraph()
        nodes = []
        edges = []

        for n in self.config["csv"]:
            k = n["filetype"]
            nodes_table = [(f"table:{k}", {"type": "table", "label": k})]
            vcols = n["vertex_collections"]
            for item in vcols:
                cname = item["type"]
                ref_fields = self.config["vertex_collections"][cname]["index"]
                if "map_fields" in item:
                    cmap = item["map_fields"]
                else:
                    cmap = dict()
                fields_collection_complementary = set(ref_fields) - set(cmap.values())
                cmap.update({qq: qq for qq in list(fields_collection_complementary)})

                index_fields = self.config["vertex_collections"][cname]["index"]

                node_collection = (
                    f"collection:{cname}",
                    {"type": "vcollection", "label": cname},
                )
                nodes_fields_table = [
                    (f"table:field:{kk}", {"type": "field", "label": kk})
                    for kk in cmap.keys()
                ]
                nodes_fields_collection = [
                    (
                        f"collection:field:{kk}",
                        {
                            "type": "def_field" if kk in index_fields else "field",
                            "label": kk,
                        },
                    )
                    for kk in cmap.values()
                ]
                edges_fields = [
                    (f"table:field:{kk}", f"collection:field:{vv}")
                    for kk, vv in cmap.items()
                ]
                edge_table_fields = [(f"table:{k}", q) for q, _ in nodes_fields_table]
                edge_collection_fields = [
                    (q, node_collection[0]) for q, _ in nodes_fields_collection
                ]
                nodes += (
                    nodes_table
                    + [node_collection]
                    + nodes_fields_table
                    + nodes_fields_collection
                )
                edges += edges_fields + edge_table_fields + edge_collection_fields

        g.add_nodes_from(nodes)
        g.add_edges_from(edges)

        for n in g.nodes():
            props = g.nodes()[n]
            upd_dict = props.copy()
            if "type" in upd_dict:
                upd_dict["shape"] = map_type2shape[props["type"]]
                upd_dict["color"] = map_type2color[props["type"]]
            if "label" in upd_dict:
                upd_dict["forcelabel"] = True
            upd_dict["style"] = "filled"
            for k, v in upd_dict.items():
                g.nodes[n][k] = v

        for e in g.edges(data=True):
            s, t, _ = e
            target_props = g.nodes[s]
            upd_dict = {
                # "style": edge_status[target_props["type"]],
                "arrowhead": "vee"
            }
            for k, v in upd_dict.items():
                g.edges[s, t][k] = v

        ag = nx.nx_agraph.to_agraph(g)

        for k, props in self.config["vertex_collections"].items():
            level_index = [f"collection:field:{item}" for item in props["index"]]
            index_subgraph = ag.add_subgraph(level_index, name=f"cluster_{k[:3]}:def")
            index_subgraph.node_attr["style"] = "filled"
            index_subgraph.node_attr["label"] = "definition"

        ag.draw(
            os.path.join(self.figgpath, f"{self.prefix}_source2vc_detailed.pdf"),
            "pdf",
            prog="dot",
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--config", default=None, help="config file name")

    args = parser.parse_args()

    plotter = SchemaPlotter(args.config)
    plotter.plot_vc2fields()
    plotter.plot_source2vc()
    plotter.plot_vc2vc()
    if plotter.type == "csv":
        plotter.plot_source2vc_detailed()
