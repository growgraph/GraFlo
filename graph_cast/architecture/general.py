import importlib

from graph_cast.architecture.schema import VertexConfig, GraphConfig


class Configurator:
    def __init__(self, config):
        self.vertex_config = VertexConfig(config["vertex_collections"])
        self.graph_config = GraphConfig(
            config["edge_collections"],
            self.vertex_config.name,
            config["json"] if "json" in config else None,
        )


class Transform:
    _module = None
    _foo = None
    _params = dict()
    _outputs = ()
    _inputs = ()

    def __init__(self, **kwargs):
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


def transform_foo(transform, doc):
    upd = {}
    if transform.input:
        if transform.output:
            args = [doc[k] for k in transform.input]
            upd = {k: v for k, v in zip(transform.output, transform(*args))}
        else:
            args = [doc[k] for k in transform.input]
            upd = transform(*args)
    # elif "fields" in transform:
    #     upd = {k: transform.foo(v) for k, v in doc.items() if k in transform.foo["fields"]}
    return upd