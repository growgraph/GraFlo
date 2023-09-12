import argparse
import os
from itertools import product

import networkx as nx

from graph_cast.architecture import (
    DataSourceType,
    JConfigurator,
    TConfigurator,
)
from graph_cast.util import ResourceHandler

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

edge_status = {"vcollection": "solid", "table": "solid"}


def knapsack(weights, ks_size=7):
    """
    split a set of weights into bag (groups) of total weight of at most threshold weight
    :param weights:
    :param ks_size:
    :return:
    """
    pp = sorted(list(zip(range(len(weights)), weights)), key=lambda x: x[1])
    print(pp)
    acc = []
    if pp[-1][1] > ks_size:
        raise ValueError("One of the items is larger than the knapsack")

    while pp:
        w_item = []
        w_item += [pp.pop()]
        ww_item = sum([l for _, l in w_item])
        while ww_item < ks_size:
            cnt = 0
            for j, item in enumerate(pp[::-1]):
                diff = ks_size - item[1] - ww_item
                if diff >= 0:
                    cnt += 1
                    w_item += [pp.pop(len(pp) - j - 1)]
                    ww_item += w_item[-1][1]
                else:
                    break
            if ww_item >= ks_size or cnt == 0:
                acc += [w_item]
                break
    acc_ret = [[y for y, _ in subitem] for subitem in acc]
    return acc_ret


class SchemaPlotter:
    def __init__(self, config_filename, fig_path):
        self.fig_path = fig_path

        self.config = ResourceHandler.load(fpath=config_filename)

        self.type: DataSourceType

        if DataSourceType.JSON in self.config:
            self.type = DataSourceType.JSON
            self.conf = JConfigurator(self.config)
        elif DataSourceType.TABLE in self.config:
            self.type = DataSourceType.TABLE
            self.conf = TConfigurator(self.config)
        else:
            raise KeyError(f"Configured to plot json or table mapper schemas")

        self.name = self.conf.name
        self.prefix = f"{self.name}_{self.type}"

    def plot_vc2fields(self):
        g = nx.DiGraph()
        nodes = []
        edges = []
        vconf = self.conf.vertex_config
        for k in vconf.collections:
            index_fields = vconf.index(k)
            fields = vconf.fields(k)
            nodes_collection = [(k, {"type": "vcollection"})]
            nodes_fields = [
                (
                    f"{k}:{item}",
                    {
                        "type": (
                            "def_field" if item in index_fields else "field"
                        ),
                        "label": item,
                    },
                )
                for item in fields
            ]
            nodes += nodes_collection
            nodes += nodes_fields
            edges += [
                (x[0], y[0])
                for x, y in product(nodes_collection, nodes_fields)
            ]

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
        # ccs = list(nx.weakly_connected_components(g))
        # ccs_sizes = [len(cc) - 1 for cc in ccs]
        # clusters = knapsack(ccs_sizes, 7)
        #
        # vclusters = [[x for i in c for x in ccs[i]] for c in clusters]
        #
        # index_vertices = [
        #     f"{k}:{item}"
        #     for k, props in self.config["vertex_collections"].items()
        #     for item in props["index"]
        # ]
        #
        # vclusters_index = [sorted([v for v in g if v in index_vertices]) for g in vclusters]
        #
        # if any([len(item) == 0 for item in vclusters_index]):
        #     raise "Some cluster has no index fields"
        #
        # level_one = [x[0] for x in vclusters_index]
        # ag.add_subgraph(level_one, rank='same')

        for k in vconf.collections:
            level_index = [f"{k}:{item}" for item in vconf.index(k)]
            index_subgraph = ag.add_subgraph(
                level_index, name=f"cluster_{k}:def"
            )
            index_subgraph.node_attr["style"] = "filled"
            index_subgraph.node_attr["label"] = "definition"

        ag = ag.unflatten("-l 5 -f -c 3")
        ag.draw(
            os.path.join(self.fig_path, f"{self.prefix}_vc2fields.pdf"),
            "pdf",
            prog="dot",
        )

    def plot_source2vc(self):
        """
        draw map of source vertices (nodes of json or table files) to vertex collections


        """
        nodes = []
        if self.type == DataSourceType.JSON:
            g = nx.DiGraph()
            edges = list(self.conf.graph_config.all_edges)
            for ee in edges:
                for n in ee:
                    nodes += [(n, {"type": "vcollection"})]

            for nid, weight in nodes:
                g.add_node(nid, **weight)
        elif self.type == DataSourceType.TABLE:
            g = nx.MultiDiGraph()
            edges = []
            for k, local_vertex_cols in self.conf.modes2collections.items():
                nodes_table = [(k, {"type": "table"})]
                nodes_collection = [
                    (vc, {"type": "vcollection"})
                    for vc in local_vertex_cols.collections
                ]
                nodes += nodes_table
                nodes += nodes_collection
                edges += [
                    (nt[0], nc[0])
                    for nt, nc in product(nodes_table, nodes_collection)
                ]

            g.add_nodes_from(nodes)
        else:
            raise KeyError(f"Supported types : {DataSourceType}")

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
            os.path.join(self.fig_path, f"{self.prefix}_source2vc.pdf"),
            "pdf",
            prog="dot",
        )

    def plot_vc2vc(self, prune_leaves=False):
        """
            vc -> vc
        :return:
        """
        g = nx.MultiDiGraph()
        nodes = []
        edge_labels = []
        if self.type == DataSourceType.JSON:
            # edges = list(self.conf.graph_config.all_edges)

            edges = [
                (a, b, {"label": label})
                for a, b, label in self.conf.graph_config.edges_triples
            ]

            for ee in edges:
                for n in ee[:2]:
                    nodes += [(n, {"type": "vcollection"})]

            for nid, weight in nodes:
                g.add_node(nid, **weight)
        elif self.type == DataSourceType.TABLE:
            nodes = []
            edges = []
            for mode, local_vertex_cols in self.conf.modes2collections.items():
                nodes_collection = [
                    (vcol, {"type": "vcollection"})
                    for vcol in local_vertex_cols.collections
                ]
                nodes += nodes_collection

            for _, item in self.conf.modes2graphs.items():
                edges += [(u, v) for u, v in item]

        g.add_nodes_from(nodes)
        g.add_edges_from(edges)

        if prune_leaves:
            out_deg = g.out_degree()
            in_deg = g.in_degree()

            nodes_to_remove = set([k for k, v in out_deg if v == 0]) & set(
                [k for k, v in in_deg if v < 2]
            )
            g.remove_nodes_from(nodes_to_remove)

        for n in g.nodes():
            props = g.nodes()[n]
            upd_dict = {
                "shape": map_type2shape[props["type"]],
                "color": map_type2color[props["type"]],
                "style": "filled",
            }
            for k, v in upd_dict.items():
                g.nodes[n][k] = v

        for e in g.edges:
            s, t, ix = e
            target_props = g.nodes[s]
            upd_dict = {
                "style": edge_status[target_props["type"]],
                "arrowhead": "vee",
            }
            for k, v in upd_dict.items():
                g.edges[s, t, ix][k] = v

        ag = nx.nx_agraph.to_agraph(g)
        # ['neato' | 'dot' | 'twopi' | 'circo' | 'fdp' | 'nop']
        ag.draw(
            os.path.join(self.fig_path, f"{self.prefix}_vc2vc.pdf"),
            "pdf",
            prog="dot",
        )

    def plot_source2vc_detailed(self):
        # TODO adjust to TConfig

        """
            source (json vertex or table) -> source fields -> vertex collection fields -> vertex collection

        :return:
        """

        g = nx.DiGraph()
        nodes = []
        edges = []

        for table_name in self.conf.table_config.tables:
            nodes_table = [
                (f"table:{table_name}", {"type": "table", "label": table_name})
            ]
            table_maps = self.conf.modes2collections[table_name]
            for vcol_name in self.conf.table_config.vertices(table_name):
                index = self.conf.vertex_config.index(vcol_name)
                ref_fields = index.fields
                maps = table_maps._vcollections[vcol_name]
                cmap = maps[0]._raw_map
                fields_collection_complementary = set(ref_fields) - set(
                    cmap.values()
                )
                cmap.update(
                    {qq: qq for qq in list(fields_collection_complementary)}
                )

                node_collection = (
                    f"collection:{vcol_name}",
                    {"type": "vcollection", "label": vcol_name},
                )
                nodes_fields_table = [
                    (f"table:field:{kk}", {"type": "field", "label": kk})
                    for kk in cmap.keys()
                ]
                nodes_fields_collection = [
                    (
                        f"collection:field:{kk}",
                        {
                            "type": (
                                "def_field" if kk in ref_fields else "field"
                            ),
                            "label": kk,
                        },
                    )
                    for kk in cmap.values()
                ]
                edges_fields = [
                    (f"table:field:{kk}", f"collection:field:{vv}")
                    for kk, vv in cmap.items()
                ]
                edge_table_fields = [
                    (f"table:{table_name}", q) for q, _ in nodes_fields_table
                ]
                edge_collection_fields = [
                    (q, node_collection[0]) for q, _ in nodes_fields_collection
                ]
                nodes += (
                    nodes_table
                    + [node_collection]
                    + nodes_fields_table
                    + nodes_fields_collection
                )
                edges += (
                    edges_fields + edge_table_fields + edge_collection_fields
                )
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
            g.nodes[s]
            upd_dict = {
                # "style": edge_status[target_props["type"]],
                "arrowhead": "vee"
            }
            for k, v in upd_dict.items():
                g.edges[s, t][k] = v

        ag = nx.nx_agraph.to_agraph(g)

        for vcol_name in self.conf.vertex_config.collections:
            index = self.conf.vertex_config.index(vcol_name).fields
            level_index = [f"collection:field:{item}" for item in index]
            index_subgraph = ag.add_subgraph(
                level_index, name=f"cluster_{vcol_name[:3]}:def"
            )
            index_subgraph.node_attr["style"] = "filled"
            index_subgraph.node_attr["label"] = "definition"

        ag.draw(
            os.path.join(
                self.fig_path, f"{self.prefix}_source2vc_detailed.pdf"
            ),
            "pdf",
            prog="dot",
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-c", "--config-path", default=None, help="path to config file"
    )

    parser.add_argument(
        "-f",
        "--figure-output-path",
        default=None,
        help="path to output the figure",
    )
    parser.add_argument(
        "-p",
        "--prune-low-degree-nodes",
        action="store_true",
        help="prune low degree nodes for vc2vc",
    )

    args = parser.parse_args()

    plotter = SchemaPlotter(args.config_path, args.figure_output_path)
    plotter.plot_vc2fields()
    plotter.plot_source2vc()
    plotter.plot_vc2vc(prune_leaves=args.prune_low_degree_nodes)
    if plotter.type == DataSourceType.TABLE:
        plotter.plot_source2vc_detailed()
