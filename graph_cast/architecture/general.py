from __future__ import annotations

import abc
import logging
from collections import Iterable, defaultdict
from typing import TypeVar

from graph_cast.architecture.schema import GraphConfig, VertexConfig

logger = logging.getLogger(__name__)

ConfiguratorType = TypeVar("ConfiguratorType", bound="Configurator")


class Configurator:
    def __init__(self, config):
        self.vertex_config = VertexConfig(config["vertex_collections"])
        edge_collections = (
            config["edge_collections"] if "edge_collections" in config else ()
        )
        self.graph_config = GraphConfig(
            edge_collections,
            self.vertex_config,
            config["json"] if "json" in config else None,
        )
        self.current_fname: str | None = None

    @abc.abstractmethod
    def set_current_resource_name(self, resource):
        pass

    @property
    def encoding(self):
        return "utf-8"

    @property
    def current_graphs(self):
        return []

    @property
    def current_collections(self):
        return []

    @property
    def current_transformations(self):
        return []

    def graph(self, u, v):
        return self.graph_config.graph(u, v)


class TransformException(BaseException):
    def __init__(self, *args, **kwargs):
        super.__init__(*args, **kwargs)


def transform_foo(transform, doc):
    upd = {}
    if transform.input:
        try:
            if transform.output:
                args = [doc[k] for k in transform.input]
                transform_result = transform(*args)
                if isinstance(transform_result, Iterable):
                    upd = {
                        k: v
                        for k, v in zip(transform.output, transform_result)
                    }
                else:
                    upd = {transform.output[0]: transform_result}
            else:
                args = [doc[k] for k in transform.input]
                upd = {k: v for k, v in zip(transform.input, transform(*args))}
        except:  # ValueError(
            # f" application of transform_foo for doc {doc}; and transform {transform}"
            # ) as e:

            logger.debug(
                f" application of transform_foo for doc {doc}; and transform"
                f" {transform}"
            )
            if transform.output:
                upd = {
                    **{f"_status@{k}": False for k in transform.output},
                    **{k: False for k in transform.output},
                }
            else:
                upd = {
                    **{f"_status@{k}": False for k in transform.input},
                    **{k: False for k in transform.input},
                }
    # elif "fields" in transform:
    #     upd = {k: transform.foo(v) for k, v in doc.items() if k in transform.foo["fields"]}
    return upd


class Mapper:
    def __init__(self, **kwargs):
        self._filename = None
        self._filename_field = None
        self._request_filename = False
        if "map" in kwargs:
            self._raw_map = kwargs["map"]
        else:
            self._raw_map = {}
        self._process_maps()
        self._check_map_splitter()
        self._possible_keys = set(self._map) | set(self._map_splitter)
        if "_filename" in self._map:
            if "filename" in kwargs:
                self._filename = kwargs["filename"]
            self._request_filename = True
            self._filename_field = self._map["_filename"]
            del self._map["_filename"]

    def _process_maps(self):
        self._map = {
            k: v for k, v in self._raw_map.items() if isinstance(v, str)
        }
        self._map_splitter = {
            k: v for k, v in self._raw_map.items() if not isinstance(v, str)
        }

    def _check_map_splitter(self):
        for k, item in self._map_splitter.items():
            if not isinstance(item, dict):
                raise TypeError(
                    f" self._raw_map should be a dict : {self._raw_map}"
                )
            if "key" not in item:
                raise KeyError(
                    f" item should contain 'key' and 'value' : {item}"
                )
            if "value" not in item:
                item["value"] = "value"

    def _update_filename(self, filename):
        self._filename = filename

    def update(self, **kwargs):
        for k, v in kwargs.items():
            if self._request_filename and k == "filename":
                setattr(self, f"_{k}", v.split("/")[-1].split(".")[0])

    @property
    def input(self):
        return self._map.keys()

    @property
    def input_split(self):
        return len(self._map_splitter) > 0

    @property
    def dynamic_transformations(self):
        return self._request_filename

    @property
    def active(self):
        return self.input or self.input_split or self.dynamic_transformations

    def __call__(self, item, filename=None):
        acc = {v: item[k] for k, v in self._map.items()}
        acc.update(
            {
                f"_status@{k}": item[f"_status@{k}"]
                for k in self._map
                if f"_status@{k}" in item
            }
        )
        # if self._request_filename and filename is not None:
        #     self._update_filename(filename)
        if self._request_filename and self._filename_field:
            acc[self._filename_field] = self._filename
        for k, cmap in self._map_splitter.items():
            cm_key, cm_value = cmap["key"], cmap["value"]
            acc[cm_key] = k
            acc[cm_value] = item[k]
            if f"_status@{k}" in item:
                acc.update({f"_status@{cm_value}": item[f"_status@{k}"]})
        return acc

    def __str__(self):
        return f"{id(self)}:  {self._map} : {self._map_splitter}"

    __repr__ = __str__


class LocalVertexCollections:
    def __init__(self, inp):
        self._vcollections = defaultdict(list)
        for cc in inp:
            # TODO and type is allowed
            if "type" in cc:
                self._vcollections[cc["type"]] += [Mapper(**cc)]

    def __iter__(self):
        return (
            (k, m) for k in self.collections for m in self._vcollections[k]
        )

    @property
    def collections(self):
        return self._vcollections.keys()

    def update_mappers(self, **kwargs):
        for k, m in self:
            m.update(**kwargs)
