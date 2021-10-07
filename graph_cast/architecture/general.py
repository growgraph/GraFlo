import importlib
from collections import defaultdict
from graph_cast.architecture.schema import VertexConfig, GraphConfig


class Configurator:
    def __init__(self, config):
        self.vertex_config = VertexConfig(config["vertex_collections"])
        self.graph_config = GraphConfig(
            config["edge_collections"],
            self.vertex_config.dbname,
            config["json"] if "json" in config else None,
        )


class Transform:
    def __init__(self, **kwargs):
        self._module = None
        self._foo = None
        self._params = dict()
        self._outputs = ()
        self._inputs = ()
        self._init_module(**kwargs)

        try:
            self._foo = getattr(self._module, kwargs["foo"])
        except:
            raise ValueError
        if "params" in kwargs:
            self._params = kwargs["params"]
        if not isinstance(self._params, dict):
            raise TypeError("params should be dict-like")
        if "input" in kwargs:
            self._inputs = kwargs["input"]
        if "output" in kwargs:
            self._outputs = kwargs["output"]

    def _init_module(self, **kwargs):
        if "module" in kwargs:
            self._module = importlib.import_module(kwargs["module"])
        elif "class" in kwargs:
            self._module = eval(kwargs["module"])
        else:
            raise KeyError("Either module or class keys should be present")

    def __call__(self, *nargs, **kwargs):
        return self._foo(*nargs, **kwargs, **self._params)

    @property
    def input(self):
        return self._inputs

    @property
    def output(self):
        return self._outputs

    def __str__(self):
        return f"{id(self)} | {self._foo} {self._inputs} -> {self._outputs}"

    __repr__ = __str__


def transform_foo(transform, doc):
    upd = {}
    if transform.input:
        if transform.output:
            args = [doc[k] for k in transform.input]
            upd = {k: v for k, v in zip(transform.output, transform(*args))}
        else:
            args = [doc[k] for k in transform.input]
            upd = {k: v for k, v in zip(transform.input, transform(*args))}
    # elif "fields" in transform:
    #     upd = {k: transform.foo(v) for k, v in doc.items() if k in transform.foo["fields"]}
    return upd


class Mapper:
    def __init__(self, **kwargs):
        self._filename = None
        self._filename_field = None
        self._request_filename = False
        if "map" in kwargs:
            self._map = kwargs["map"]
        else:
            self._map = {}
        self._init_key_field()
        if "_filename" in self._map:
            if "filename" in kwargs:
                self._filename = kwargs["filename"]
            self._request_filename = True
            self._filename_field = self._map["_filename"]
            del self._map["_filename"]

    def _init_key_field(self):
        if "@key" in self._map:
            self._key_field = self._map["@key"]
            del self._map["@key"]
        else:
            self._key_field = None

    def _update_filename(self, filename):
        self._filename = filename

    def update(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, f"_{k}", v)

    @property
    def input(self):
        return self._map.keys()

    def __call__(self, item, filename=None):
        acc = {v: item[k] for k, v in self._map.items()}
        if self._request_filename and filename is not None:
            self._update_filename(filename)
        if self._request_filename and self._filename_field:
            acc[self._filename_field] = self._filename
        if self._key_field is not None:
            acc[self._key_field] = list(item.keys())[0]
        return acc

    def __str__(self):
        return f"{id(self)}:  {self._map}"

    __repr__ = __str__


class LocalVertexCollections:
    def __init__(self, inp):
        self._vcollections = defaultdict(list)
        for cc in inp:
            # TODO and type is allowed
            if "type" in cc:
                # if "map" not in cc:
                #     print(cc["type"])
                self._vcollections[cc["type"]] += [Mapper(**cc)]

    def __iter__(self):
        return ((k, m) for k in self.collections for m in self._vcollections[k])

    @property
    def collections(self):
        return self._vcollections.keys()

    def update_mappers(self, **kwargs):
        for k, m in self:
            m.update(**kwargs)
