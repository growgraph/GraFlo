from __future__ import annotations

from collections import defaultdict

from graph_cast.architecture.ptree import MapperNode, NodeType, ParsingTree
from graph_cast.architecture.schema import Edge, VertexConfig


class GraphConfig:
    def __init__(self, econfig, vconfig: VertexConfig):
        """

        :param econfig: edges config : direct definitions of edges
        :param vconfig: specification of vcollections
        """
        self._edges: dict[tuple[str, str], Edge] = dict()

        self._exclude_fields: defaultdict[str, list] = defaultdict(list)

        self._init_edges(econfig, vconfig)
        self._init_extra_edges(econfig, vconfig)
        self._init_exclude()

    def _init_edges(self, config, vconf: VertexConfig):
        if "main" in config:
            for e in config["main"]:
                edge = Edge(e, vconf)
                self._edges.update({edge.edge_name_dyad: edge})

    def _init_extra_edges(self, config, vconf: VertexConfig):
        if "extra" in config:
            for e in config["extra"]:
                edge = Edge(e, vconf, direct=False)
                self._edges.update({edge.edge_name_dyad: edge})

    def _init_exclude(self):
        for (v, w), e in self._edges.items():
            self._exclude_fields[v] += e.source_exclude
            self._exclude_fields[w] += e.target_exclude

    def _parse_tree_edges(
        self,
        croot: MapperNode,
        edge_accumulator: defaultdict[tuple[str, str], list[Edge]],
    ):
        """
        extract edge definition and edge fields from definition dict
        :param croot:
        :param edge_accumulator:
        :return:
        """
        if croot.type == NodeType.EDGE:
            edge_accumulator[croot.edge.edge_name_dyad] += [croot.edge]
        for c in croot.children:
            self._parse_tree_edges(c, edge_accumulator)
        return edge_accumulator

    def parse_edges(
        self,
        pt: ParsingTree,
    ):
        """
        extract edge definition and edge fields from definition dict
        :return:
        """
        acc_edges: defaultdict[tuple[str, str], list[Edge]] = defaultdict(list)
        normalized_edges: dict[tuple[str, str], Edge] = dict()
        acc_edges = self._parse_tree_edges(pt.root, acc_edges)

        for k, item in acc_edges.items():
            if item:
                normalized_edges[k] = item[0]
            for edef in item[1:]:
                normalized_edges[k] += edef

        self._edges.update(normalized_edges)

    def graph(self, u, v) -> Edge:
        return self._edges[u, v]

    @property
    def edges(self):
        return list([k for k, v in self._edges.items() if v.type == "direct"])

    @property
    def extra_edges(self):
        return list(
            [k for k, v in self._edges.items() if v.type == "indirect"]
        )

    @property
    def all_edges(self):
        return list(self._edges)

    def exclude_fields(self, k):
        if k in self._exclude_fields:
            return self._exclude_fields[k]
        else:
            return ()
