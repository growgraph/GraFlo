import importlib


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
            raise ValueError("could instantiate transform function")

        if "params" in kwargs:
            self._params = kwargs["params"]
        if not isinstance(self._params, dict):
            raise TypeError("params should be dict-like")
        if "input" in kwargs:
            self._inputs = kwargs["input"]
        if "output" in kwargs:
            self._outputs = kwargs["output"]
        # TODO clean up transforms mess : could be either used locally with inputs and outputs
        #                   or with fields (applied per field)
        if "fields" in kwargs:
            self._inputs = kwargs["fields"]
            self._outputs = kwargs["fields"]

    def _init_module(self, **kwargs):
        if "module" in kwargs:
            self._module = importlib.import_module(kwargs["module"])
        elif "class" in kwargs:
            self._module = eval(kwargs["class"])
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
