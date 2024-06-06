import os
from itertools import product

import networkx as nx
from suthing import FileHandle

from graph_cast.architecture import DataSourceType, Schema
from graph_cast.onto import BaseEnum


class AuxNodeType(str, BaseEnum):
    FIELD = "field"
    FIELD_DEFINITION = "field_definition"
    INDEX = "field"
    RESOURCE = "resource"
    TRANSFORM = "transform"
    VERTEX = "vertex"
    VERTEX_BLANK = "vertex_blank"


fillcolor_palette = {
    "violet": "#DDD0E5",
    "green": "#BEDFC8",
    "blue": "#B7D1DF",
    "red": "#EBA59E",
    "peach": "#FFE5B4",
}
map_type2shape = {
    AuxNodeType.RESOURCE: "box",
    AuxNodeType.VERTEX_BLANK: "box",
    AuxNodeType.FIELD_DEFINITION: "trapezium",
    AuxNodeType.TRANSFORM: "oval",
    AuxNodeType.VERTEX: "ellipse",
    AuxNodeType.INDEX: "polygon",
    AuxNodeType.FIELD: "octagon",
}

map_type2color = {
    AuxNodeType.RESOURCE: fillcolor_palette["blue"],
    # "tree": fillcolor_palette["peach"],
    AuxNodeType.FIELD_DEFINITION: fillcolor_palette["red"],
    AuxNodeType.VERTEX_BLANK: "white",
    AuxNodeType.VERTEX: fillcolor_palette["green"],
    AuxNodeType.INDEX: "orange",
    AuxNodeType.TRANSFORM: "grey",
    AuxNodeType.FIELD: fillcolor_palette["violet"],
}

edge_status = {AuxNodeType.VERTEX: "solid"}


def get_auxnode_id(ntype: AuxNodeType, label=False, vfield=False, **kwargs):
    vertex = kwargs.pop("vertex", None)
    resource = kwargs.pop("resource", None)
    vertex_shortcut = kwargs.pop("vertex_sh", None)
    resource_shortcut = kwargs.pop("resource_sh", None)
    if ntype == AuxNodeType.RESOURCE:
        resource_type = kwargs.pop("resource_type")
        if label:
            s = f"{resource}"
        else:
            s = f"{ntype}:{resource_type}:{resource}"
    elif ntype == AuxNodeType.VERTEX:
        if label:
            s = f"{vertex}"
        else:
            s = f"{ntype}:{vertex}"
    elif ntype == AuxNodeType.FIELD:
        field = kwargs.pop("field", None)
        if vfield:
            if label:
                s = f"({vertex_shortcut[vertex]}){field}"
            else:
                s = f"{ntype}:{vertex}:{field}"
        else:
            if label:
                s = f"<{resource_shortcut[resource]}>{field}"
            else:
                s = f"{ntype}:{resource}:{field}"
    elif ntype == AuxNodeType.TRANSFORM:
        inputs = kwargs.pop("inputs")
        outputs = kwargs.pop("outputs")
        t_spec = inputs + outputs
        t_key = "-".join(t_spec)
        t_label = "-".join([x[0] for x in t_spec])

        if label:
            s = f"[t]{t_label}"
        else:
            s = f"transform:{t_key}"
    return s


def lto_dict(strings):
    strings = list(set(strings))
    d = {"": strings}
    while any([len(v) > 1 for v in d.values()]):
        keys = list(d.keys())
        for k in keys:
            item = d.pop(k)
            if len(item) < 2:
                d[k] = item
            else:
                for s in item:
                    if s:
                        if k + s[0] in d:
                            d[k + s[0]].append(s[1:])
                        else:
                            d[k + s[0]] = [s[1:]]
                    else:
                        d[k] = [s]
    r = {}
    for k, v in d.items():
        if v:
            r[k] = v[0]
    return r


class SchemaPlotter:
    def __init__(self, config_filename, fig_path):
        self.fig_path = fig_path

        self.config = FileHandle.load(fpath=config_filename)

        self.type: DataSourceType

        self.conf = Schema.from_dict(self.config)

        self.name = self.conf.general.name
        # self.prefix = f"{self.name}_{self.type}"
        self.prefix = self.name

    def plot_vc2fields(self):
        g = nx.DiGraph()
        nodes = []
        edges = []
        vconf = self.conf.vertex_config
        vertex_prefix_dict = lto_dict([v for v in self.conf.vertex_config.vertex_set])

        kwargs = {"vfield": True, "vertex_sh": vertex_prefix_dict}
        for k in vconf.vertex_set:
            index_fields = vconf.index(k)
            fields = vconf.fields(k)
            kwargs["vertex"] = k
            nodes_collection = [
                (
                    get_auxnode_id(AuxNodeType.VERTEX, **kwargs),
                    {
                        "type": AuxNodeType.VERTEX,
                        "label": get_auxnode_id(
                            AuxNodeType.VERTEX, label=True, **kwargs
                        ),
                    },
                )
            ]
            nodes_fields = [
                (
                    get_auxnode_id(AuxNodeType.FIELD, field=item, **kwargs),
                    {
                        "type": (
                            AuxNodeType.FIELD_DEFINITION
                            if item in index_fields
                            else AuxNodeType.FIELD
                        ),
                        "label": get_auxnode_id(
                            AuxNodeType.FIELD, field=item, label=True, **kwargs
                        ),
                    },
                )
                for item in fields
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

        for k in vconf.vertex_set:
            level_index = [
                get_auxnode_id(
                    AuxNodeType.FIELD,
                    vertex=k,
                    field=item,
                    vfield=True,
                    vertex_sh=vertex_prefix_dict,
                )
                for item in vconf.index(k)
            ]
            index_subgraph = ag.add_subgraph(level_index, name=f"cluster_{k}:def")
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
        draw map of source vertices (nodes of json or csv files) to vertex collections


        """
        nodes = []
        g = nx.MultiDiGraph()
        edges = []
        resource_prefix_dict = lto_dict(
            [resource.name for resource in self.conf.resources]
        )
        vertex_prefix_dict = lto_dict([v for v in self.conf.vertex_config.vertex_set])
        kwargs = {"vertex_sh": vertex_prefix_dict, "resource_sh": resource_prefix_dict}

        for resource in self.conf.resources:
            kwargs["resource"] = resource.name
            kwargs["resource_type"] = resource.resource_type

            vertices = list(resource.vertex_rep.keys())
            nodes_resource = [
                (
                    get_auxnode_id(AuxNodeType.RESOURCE, **kwargs),
                    {
                        "type": AuxNodeType.RESOURCE,
                        "label": get_auxnode_id(
                            AuxNodeType.RESOURCE, label=True, **kwargs
                        ),
                    },
                )
            ]
            nodes_vertex = [
                (
                    get_auxnode_id(AuxNodeType.VERTEX, vertex=v, **kwargs),
                    {
                        "type": AuxNodeType.VERTEX,
                        "label": get_auxnode_id(
                            AuxNodeType.VERTEX, vertex=v, label=True, **kwargs
                        ),
                    },
                )
                for v in vertices
            ]
            nodes += nodes_resource
            nodes += nodes_vertex
            edges += [
                (nt[0], nc[0]) for nt, nc in product(nodes_resource, nodes_vertex)
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
            for resource, v in upd_dict.items():
                g.nodes[n][resource] = v

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
        edges = []
        for e in self.conf.edge_config.edges:
            if e.relation is not None:
                ee = (
                    get_auxnode_id(AuxNodeType.VERTEX, vertex=e.source),
                    get_auxnode_id(AuxNodeType.VERTEX, vertex=e.target),
                    {"label": e.relation},
                )
            else:
                ee = (
                    get_auxnode_id(AuxNodeType.VERTEX, vertex=e.source),
                    get_auxnode_id(AuxNodeType.VERTEX, vertex=e.target),
                )
            edges += [ee]

        for ee in self.conf.edge_config.edges:
            for v in (ee.source, ee.target):
                nodes += [
                    (
                        get_auxnode_id(AuxNodeType.VERTEX, vertex=v),
                        {
                            "type": AuxNodeType.VERTEX,
                            "label": get_auxnode_id(
                                AuxNodeType.VERTEX, vertex=v, label=True
                            ),
                        },
                    )
                ]

        for nid, weight in nodes:
            g.add_node(nid, **weight)

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
        """
            resource [treelike or rowlike] -> source fields -> vertex collection fields -> vertex collection

        :return:
        """

        g = nx.DiGraph()
        nodes = []
        edges = []
        resource_prefix_dict = lto_dict(
            [resource.name for resource in self.conf.resources]
        )
        vertex_prefix_dict = lto_dict([v for v in self.conf.vertex_config.vertex_set])
        kwargs = {"vertex_sh": vertex_prefix_dict, "resource_sh": resource_prefix_dict}
        for resource in self.conf.resources:
            kwargs["resource"] = resource.name
            kwargs["resource_type"] = resource.resource_type

            nodes_table = [
                (
                    get_auxnode_id(AuxNodeType.RESOURCE, **kwargs),
                    {
                        "type": AuxNodeType.RESOURCE,
                        "label": get_auxnode_id(
                            AuxNodeType.RESOURCE, label=True, **kwargs
                        ),
                    },
                )
            ]
            for vertex, rep in resource.vertex_rep.items():
                kwargs["vertex"] = vertex
                index = self.conf.vertex_config.index(vertex)
                node_collection = (
                    get_auxnode_id(AuxNodeType.VERTEX, **kwargs),
                    {
                        "type": AuxNodeType.VERTEX,
                        "label": get_auxnode_id(
                            AuxNodeType.VERTEX, label=True, **kwargs
                        ),
                    },
                )

                nodes_fields_resource = []
                nodes_fields_vertex = []
                nodes_transforms = []
                edges_fields = []
                edges_fields_transforms = []

                ref_fields = index.fields

                for m in rep.maps:
                    nodes_fields_resource += [
                        (
                            get_auxnode_id(AuxNodeType.FIELD, field=f, **kwargs),
                            {
                                "type": AuxNodeType.FIELD,
                                "label": get_auxnode_id(
                                    AuxNodeType.FIELD, field=f, label=True, **kwargs
                                ),
                            },
                        )
                        for f in m.keys()
                    ]
                    nodes_fields_vertex += [
                        (
                            get_auxnode_id(
                                AuxNodeType.FIELD, field=f, vfield=True, **kwargs
                            ),
                            {
                                "type": AuxNodeType.FIELD,
                                "label": get_auxnode_id(
                                    AuxNodeType.FIELD,
                                    field=f,
                                    vfield=True,
                                    label=True,
                                    **kwargs,
                                ),
                            },
                        )
                        for f in m.values()
                    ]
                    edges_fields += [
                        (
                            get_auxnode_id(AuxNodeType.FIELD, field=kk, **kwargs),
                            get_auxnode_id(
                                AuxNodeType.FIELD, field=vv, vfield=True, **kwargs
                            ),
                        )
                        for kk, vv in m.items()
                    ]

                for m in rep.transforms:
                    inputs = m[0]
                    outputs = m[1]
                    nodes_fields_resource += [
                        (
                            get_auxnode_id(AuxNodeType.FIELD, field=f, **kwargs),
                            {
                                "type": AuxNodeType.FIELD,
                                "label": get_auxnode_id(
                                    AuxNodeType.FIELD,
                                    field=f,
                                    vfield=True,
                                    label=True,
                                    **kwargs,
                                ),
                            },
                        )
                        for f in inputs
                    ]
                    nodes_fields_vertex += [
                        (
                            get_auxnode_id(
                                AuxNodeType.FIELD, field=f, vfield=True, **kwargs
                            ),
                            {
                                "type": (
                                    AuxNodeType.FIELD_DEFINITION
                                    if f in ref_fields
                                    else AuxNodeType.FIELD
                                ),
                                "label": get_auxnode_id(
                                    AuxNodeType.FIELD,
                                    field=f,
                                    vfield=True,
                                    label=True,
                                    **kwargs,
                                ),
                            },
                        )
                        for f in outputs
                    ]

                    nodes_transforms += [
                        (
                            get_auxnode_id(
                                AuxNodeType.TRANSFORM,
                                inputs=inputs,
                                outputs=outputs,
                                **kwargs,
                            ),
                            {
                                "type": AuxNodeType.TRANSFORM,
                                "label": get_auxnode_id(
                                    AuxNodeType.TRANSFORM,
                                    inputs=inputs,
                                    outputs=outputs,
                                    label=True,
                                    **kwargs,
                                ),
                            },
                        )
                    ]

                    edges_fields += [
                        (
                            get_auxnode_id(AuxNodeType.FIELD, field=f, **kwargs),
                            get_auxnode_id(
                                AuxNodeType.TRANSFORM,
                                inputs=inputs,
                                outputs=outputs,
                                **kwargs,
                            ),
                        )
                        for f in inputs
                    ]

                    edges_fields += [
                        (
                            get_auxnode_id(
                                AuxNodeType.TRANSFORM,
                                inputs=inputs,
                                outputs=outputs,
                                **kwargs,
                            ),
                            get_auxnode_id(
                                AuxNodeType.FIELD, field=f, vfield=True, **kwargs
                            ),
                        )
                        for f in outputs
                    ]

                trivial_fields = {
                    kk
                    for kk in rep.fields
                    if not any([kk in t[0] + t[1] for t in rep.transforms])
                }

                nodes_fields_resource += [
                    (
                        get_auxnode_id(AuxNodeType.FIELD, field=f, **kwargs),
                        {
                            "type": AuxNodeType.FIELD,
                            "label": get_auxnode_id(
                                AuxNodeType.FIELD, field=f, label=True, **kwargs
                            ),
                        },
                    )
                    for f in trivial_fields
                ]

                nodes_fields_vertex += [
                    (
                        get_auxnode_id(
                            AuxNodeType.FIELD, field=f, vfield=True, **kwargs
                        ),
                        {
                            "type": AuxNodeType.FIELD,
                            "label": get_auxnode_id(
                                AuxNodeType.FIELD,
                                field=f,
                                vfield=True,
                                label=True,
                                **kwargs,
                            ),
                        },
                    )
                    for f in trivial_fields
                ]

                edge_resource_fields = [
                    (get_auxnode_id(AuxNodeType.RESOURCE, **kwargs), q)
                    for q, _ in nodes_fields_resource
                ]
                edge_collection_fields = [
                    (q, node_collection[0]) for q, _ in nodes_fields_vertex
                ]

                edges_fields += [
                    (u[0], v[0])
                    for u, v in zip(nodes_fields_resource, nodes_fields_vertex)
                ]

                nodes += (
                    nodes_table
                    + [node_collection]
                    + nodes_fields_resource
                    + nodes_fields_vertex
                    + nodes_transforms
                )
                edges += (
                    edges_fields
                    + edge_resource_fields
                    + edge_collection_fields
                    + edges_fields_transforms
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
            # g.nodes[s]
            upd_dict = {
                # "style": edge_status[target_props["type"]],
                "arrowhead": "vee"
            }
            for k, v in upd_dict.items():
                g.edges[s, t][k] = v

        ag = nx.nx_agraph.to_agraph(g)

        for vertex in self.conf.vertex_config.vertex_set:
            index = self.conf.vertex_config.index(vertex).fields
            level_index = [f"collection:field:{item}" for item in index]
            index_subgraph = ag.add_subgraph(
                level_index, name=f"cluster_{vertex[:3]}:def"
            )
            index_subgraph.node_attr["style"] = "filled"
            index_subgraph.node_attr["label"] = "definition"

        ag.draw(
            os.path.join(self.fig_path, f"{self.prefix}_source2vc_detailed.pdf"),
            "pdf",
            prog="dot",
        )
