from __future__ import annotations

import abc
import dataclasses
import logging
from collections import defaultdict
from copy import deepcopy
from itertools import chain, combinations, product
from typing import Iterator

import networkx as nx

from graph_cast.architecture.edge import Edge, EdgeConfig
from graph_cast.architecture.mapper import MapperNode
from graph_cast.architecture.onto import (
    SOURCE_AUX,
    TARGET_AUX,
    EdgeType,
    EncodingType,
    GraphEntity,
)
from graph_cast.architecture.transform import Transform
from graph_cast.architecture.vertex import (
    VertexConfig,
    VertexRepresentationHelper,
)
from graph_cast.onto import BaseDataclass, ResourceType
from graph_cast.util.merge import merge_doc_basis
from graph_cast.util.transform import pick_unique_dict

logger = logging.getLogger(__name__)


DISCRIMINANT_KEY = "__discriminant_key"


@dataclasses.dataclass
class Resource(BaseDataclass):
    name: str | None = None
    resource_type: ResourceType = ResourceType.ROWLIKE
    encoding: EncodingType = EncodingType.UTF_8
    # TODO create a test for merging collection (long term):
    #       applied when there are docs without the primary key that are merged to a main doc
    merge_collections: list[str] = dataclasses.field(default_factory=list)
    extra_weights: list[Edge] = dataclasses.field(default_factory=list)

    def __post_init__(self):
        self.vertex_rep: dict[str, VertexRepresentationHelper] = dict()

    def prepare_apply(self, **kwargs):
        self._prepare_apply(**kwargs)

    @abc.abstractmethod
    def _prepare_apply(self, **kwargs):
        pass

    @abc.abstractmethod
    def apply_doc(self, doc: dict, **kwargs):
        pass

    def normalize_unit(
        self,
        unit_doc: defaultdict[GraphEntity, list],
        vertex_config: VertexConfig,
        discriminant_key: str,
    ) -> defaultdict[GraphEntity, list]:
        """

        Args:
            unit_doc: generic : ddict
            vertex_config:
            vertex_config: discriminant_key

        Returns: defaultdict vertex and edges collections

        """

        for vertex, v in unit_doc.items():
            v = pick_unique_dict(v)
            if vertex in vertex_config.vertex_set:
                v = merge_doc_basis(v, tuple(vertex_config.index(vertex).fields))
            # TODO : fix merging
            # use case - when the same vertex is defined is different places of the incoming tree-like input (json)
            # if vertex in self.merge_collections:
            #     v = merge_documents(v, vertex_config.index(vertex).fields, discriminant_key)
            if vertex in vertex_config.vertex_set:
                for item in v:
                    item.pop(discriminant_key, None)
            unit_doc[vertex] = v

        return unit_doc


@dataclasses.dataclass
class RowResource(Resource):
    transforms: list[Transform] = dataclasses.field(default_factory=list)

    def __post_init__(self):
        super().__post_init__()
        self._vertex_tau = nx.DiGraph()
        self._transforms: dict[int, Transform] = {}

        self._vertex_tau_current: nx.DiGraph = nx.DiGraph()
        self._transforms_current: dict[int, Transform] = {}

    def _prepare_apply(self, **kwargs):
        columns = kwargs.pop("columns")
        assert columns is not None
        vertex_config = kwargs.pop("vertex_config")
        assert vertex_config is not None
        self.add_trivial_transformations(vertex_config, columns)

    def finish_init(
        self,
        vertex_config: VertexConfig,
        edge_config: None | EdgeConfig = None,
        transforms: dict[str, Transform] | None = None,
    ):
        if transforms is None:
            transforms = {}

        for tau in self.transforms:
            if tau.is_dummy:
                if tau.name in transforms:
                    tau.update(transforms[tau.name])
            self._transforms[id(tau)] = tau
            related_vertices = [
                c
                for c in vertex_config.vertex_set
                if set(vertex_config.fields(c)) & set(tau.output)
            ]
            for v in related_vertices:
                if v not in self.vertex_rep:
                    self.vertex_rep[v] = VertexRepresentationHelper(
                        name=v, fields=vertex_config.fields(v)
                    )

                if tau.functional_transform:
                    self.vertex_rep[v].transforms += [(tau.input, tau.output)]
                if tau.map:
                    self.vertex_rep[v].maps += [dict(tau.map)]

            if edge_config is not None:
                related_edges = [
                    e.edge_id
                    for e in edge_config.edges
                    if e.weights is not None
                    and (set(e.weights.direct) & set(tau.output))
                ]
            else:
                related_edges = []

            if len(related_vertices) > 1:
                if tau.image is not None and tau.image in vertex_config.vertex_set:
                    related_vertices = [tau.image]
                else:
                    logger.warning(
                        f"Multiple collections {related_vertices} are"
                        f" related to transformation {tau}, consider revising"
                        " your schema"
                    )
            self._vertex_tau.add_edges_from(
                [(c, id(tau)) for c in related_vertices + related_edges]
            )

    def add_trivial_transformations(
        self, vertex_config: VertexConfig, header_keys: list[str]
    ):
        self._transforms_current = deepcopy(self._transforms)
        self._vertex_tau_current = self._vertex_tau.copy()

        pre_vertex_fields_map = {
            vertex: set(header_keys) & set(vertex_config.fields(vertex))
            for vertex in vertex_config.vertex_set
        }

        for vertex, fs in pre_vertex_fields_map.items():
            tau_fields = self.fields(vertex)
            fields_passthrough = set(fs) - tau_fields
            if fields_passthrough:
                tau = Transform(
                    map=dict(zip(fields_passthrough, fields_passthrough)),
                    image=vertex,
                )
                self._transforms_current[id(tau)] = tau
                self._vertex_tau_current.add_edges_from([(vertex, id(tau))])

    def fetch_transforms(self, ge: GraphEntity) -> Iterator[Transform]:
        if ge in self._vertex_tau_current.nodes:
            neighbours = self._vertex_tau_current.neighbors(ge)
        else:
            return iter(())
        return (self._transforms_current[k] for k in neighbours)

    def fields(self, vertex: str | None = None) -> set[str]:
        field_sets: Iterator[set[str]]
        if vertex is None:
            field_sets = (self.fields(v) for v in self.vertex_rep.keys())
        elif vertex in self._vertex_tau.nodes:
            neighbours = self._vertex_tau.neighbors(vertex)
            field_sets = (set(self._transforms[k].output) for k in neighbours)
        else:
            return set()
        fields: set[str] = set().union(*field_sets)
        return fields

    def apply_doc(self, doc: dict, **kwargs) -> defaultdict[GraphEntity, list]:
        vertex_config: VertexConfig = kwargs.pop("vertex_config")
        edge_config = kwargs.pop("edge_config")

        predocs_transformed = row_to_vertices(doc, vertex_config, self)

        item = normalize_row(predocs_transformed, vertex_config)

        item = add_blank_collections(item, vertex_config)

        item = apply_filter(item, vertex_conf=vertex_config)

        pure_weight = extract_weights(doc, self, edge_config.edges)

        item = define_edges(
            unit=item,
            unit_weights=pure_weight,
            current_edges=edge_config.edges,
            vertex_conf=vertex_config,
        )

        return item


@dataclasses.dataclass(kw_only=True)
class TreeResource(Resource):
    root: MapperNode

    def __post_init__(self):
        super().__post_init__()
        self.resource_type = ResourceType.TREELIKE

    def finish_init(
        self,
        vc: VertexConfig,
        edge_config: EdgeConfig,
        transforms: dict[str, Transform],
    ):
        self.root.finish_init(
            vc,
            edge_config=edge_config,
            vertex_rep=self.vertex_rep,
            transforms=transforms,
        )
        for e in self.extra_weights:
            e.finish_init(vc)

    def _prepare_apply(self, **kwargs):
        pass

    def apply_doc(self, doc: dict, **kwargs) -> defaultdict[GraphEntity, list]:
        vertex_config = kwargs.pop("vertex_config")
        discriminant_key = kwargs.pop("discriminant_key", DISCRIMINANT_KEY)

        acc: defaultdict[GraphEntity, list] = defaultdict(list)
        acc = self.root.apply(
            doc,
            vertex_config,
            acc,
            discriminant_key,
        )
        acc = self.normalize_unit(acc, vertex_config, discriminant_key)

        return acc


@dataclasses.dataclass
class ResourceHolder(BaseDataclass):
    row_likes: list[RowResource] = dataclasses.field(default_factory=list)
    tree_likes: list[TreeResource] = dataclasses.field(default_factory=list)

    def finish_init(
        self, vc: VertexConfig, ec: EdgeConfig, transforms: dict[str, Transform]
    ):
        for r in self.tree_likes:
            r.finish_init(vc, edge_config=ec, transforms=transforms)
        for r in self.row_likes:
            r.finish_init(vertex_config=vc, edge_config=ec, transforms=transforms)

    def __iter__(self):
        return chain(self.row_likes, self.tree_likes)


def row_to_vertices(
    doc: dict, vc: VertexConfig, rr: RowResource
) -> defaultdict[GraphEntity, list]:
    """

        doc gets transformed and mapped onto vertices

    :param doc: {k: v}
    :param vc:
    :param rr:
    :return: { vertex: [doc]}
    """

    docs: defaultdict[GraphEntity, list] = defaultdict(list)
    for vertex in vc.vertices:
        docs[vertex.name] += [
            tau(doc, __return_doc=True) for tau in rr.fetch_transforms(vertex.name)
        ]
    return docs


def normalize_row(unit, vc: VertexConfig) -> defaultdict[GraphEntity, list]:
    doc_upd: defaultdict[GraphEntity, list] = defaultdict(list)
    for k, item in unit.items():
        doc_upd[k] = merge_doc_basis(item, tuple(vc.index(k).fields))
    return doc_upd


def add_blank_collections(
    unit: defaultdict[GraphEntity, list[dict]], vertex_conf: VertexConfig
) -> defaultdict[GraphEntity, list[dict]]:
    # add blank collections
    for vertex in vertex_conf.blank_vertices:
        # if blank collection is in batch - add it
        if vertex not in unit:
            unit[vertex] = [{}]
    return unit


def apply_filter(
    unit: defaultdict[GraphEntity, list[dict]], vertex_conf: VertexConfig
) -> defaultdict[GraphEntity, list[dict]]:
    for vertex, doc_list in unit.items():
        if vertex_conf.filters(vertex):
            unit[vertex] = [
                doc
                for doc in doc_list
                if all(cfilter(doc) for cfilter in vertex_conf.filters(vertex))
            ]
    return unit


def extract_weights(
    doc: dict, row_resource: RowResource, edges: list[Edge]
) -> defaultdict[GraphEntity, list]:
    doc_upd: defaultdict[GraphEntity, list] = defaultdict(list)
    for e in edges:
        for tau in row_resource.fetch_transforms(e.edge_id):
            doc_upd[e.edge_id] += [tau(doc, __return_doc=True)]
    return doc_upd


def define_edges(
    unit: defaultdict[GraphEntity, list[dict]],
    unit_weights: defaultdict[GraphEntity, list[dict]],
    current_edges: list[Edge],
    vertex_conf: VertexConfig,
) -> defaultdict[GraphEntity, list[dict]]:
    for e in current_edges:
        u, v, _ = e.source, e.target, e.relation
        # blank_collections : db ids have to be retrieved to define meaningful edges
        if not (u in vertex_conf.blank_vertices or v in vertex_conf.blank_vertices):
            if e.type == EdgeType.DIRECT:
                ziter: product | combinations
                if u != v:
                    ziter = product(unit[u], unit[v])
                else:
                    ziter = combinations(unit[u], r=2)

                for udoc, vdoc in ziter:
                    edoc = {SOURCE_AUX: udoc, TARGET_AUX: vdoc}
                    if e.weights is not None:
                        # weights_direct = {
                        #     f: cbatch[f] for f in e.weights.direct
                        # }

                        for vertex_weight in e.weights.vertices:
                            if vertex_weight.name == u:
                                cbatch = udoc
                            elif vertex_weight.name == v:
                                cbatch = vdoc
                            else:
                                continue
                            weights = {f: cbatch[f] for f in vertex_weight.fields}
                            edoc.update(weights)
                    for ud in unit_weights[e.edge_id]:
                        edoc.update(ud)
                    unit[e.edge_id].append(edoc)
    return unit
