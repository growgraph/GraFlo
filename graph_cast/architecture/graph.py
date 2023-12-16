from __future__ import annotations

from collections import defaultdict
from typing import Iterator

from graph_cast.architecture.onto import EdgeType
from graph_cast.architecture.ptree import MapperNode, NodeType, ParsingTree
from graph_cast.architecture.schema import Edge, VertexConfig
from graph_cast.architecture.util import strip_prefix

EdgeName = tuple[str, str]


class GraphConfig:
    def __init__(self, econfig, vconfig: VertexConfig):
        """

        :param econfig: edges config : direct definitions of edges
        :param vconfig: specification of vcollections
        """
        self._edges: defaultdict[EdgeName, list[Edge]] = defaultdict(list)

        self._exclude_fields: defaultdict[str, list] = defaultdict(list)

        econfig = strip_prefix(econfig, "~")
        self._init_edges(econfig, vconfig)
        self._init_extra_edges(econfig, vconfig)
        self._init_exclude()

    def _init_edges(self, config, vconf: VertexConfig):
        if "main" in config:
            for e in config["main"]:
                edge = Edge(e, vconf)
                self.update_edges(edge, update_first=False)

    def _init_extra_edges(self, config, vconf: VertexConfig):
        if "extra" in config:
            for e in config["extra"]:
                edge = Edge(e, vconf, direct=False)
                self.update_edges(edge, update_first=False)

    def _init_exclude(self):
        for (v, w), edges in self._edges.items():
            for e in edges:
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
        acc_edges = self._parse_tree_edges(pt.root, acc_edges)

        for k, item in acc_edges.items():
            for ee in item:
                self.update_edges(ee)

    def update_edges(self, edef: Edge, update_first=True):
        if update_first and self._edges[edef.edge_name_dyad]:
            self._edges[edef.edge_name_dyad][0] += edef
        else:
            self._edges[edef.edge_name_dyad] += [edef]

    def graph(self, u, v, ix=0) -> Edge:
        if len(self._edges[u, v]) == 0:
            raise ValueError(f"edge {u}, {v} absent in GraphConfig")
        ix = min([len(self._edges[u, v]) - 1, ix])
        ix = max([0, ix])
        return self._edges[u, v][ix]

    @property
    def direct_edges(self) -> list[EdgeName]:
        edges = []
        for k, item in self._edges.items():
            if any([v.type == EdgeType.DIRECT for v in item]):
                edges += [k]
        return edges

    @property
    def extra_edges(self) -> list[Edge]:
        edges = []
        for k, item in self._edges.items():
            for edge in item:
                if edge.type == EdgeType.INDIRECT:
                    edges += [edge]
        return edges

    @property
    def all_edges(self) -> Iterator[EdgeName]:
        return (e for e in self._edges)

    @property
    def edges_triples(self):
        acc = []
        for pair, edges in self._edges.items():
            for e in edges:
                acc += [(*pair, e.relation)]
        return acc

    @property
    def vertices(self) -> list[str]:
        vs = []
        for u, v in self._edges:
            vs += [u, v]
        vs = list(set(vs))
        return vs

    def all_edge_definitions(self):
        for k, item in self._edges.items():
            for e in item:
                yield e

    def exclude_fields(self, k):
        if k in self._exclude_fields:
            return self._exclude_fields[k]
        else:
            return ()

    def edge_projection(self, vertices) -> list[EdgeName]:
        enames = []
        for u, v in self._edges:
            if u in vertices and v in vertices:
                enames += [(u, v)]
        return enames

    def weight_raw_fields(self) -> set:
        fields = set()
        for e, item in self._edges.items():
            efields = {f for edef in item for f in edef.weight_fields}
            fields |= efields
        return fields
