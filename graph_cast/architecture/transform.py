import importlib
import logging
from typing import Iterable

logger = logging.getLogger(__name__)


class TransformException(BaseException):
    pass


class Transform:
    def __init__(self, **kwargs):
        self._module = None
        self._class = None
        self._foo = None

        module_name = kwargs.pop("module", None)
        class_name = kwargs.pop("class", None)
        foo_name = kwargs.pop("foo", None)

        self._init_foo(module_name, class_name, foo_name)

        self._params = dict()

        self._inputs: tuple[str, ...] = kwargs.pop("input", tuple())
        self._inputs = self._tuple_it(self._inputs)

        self._outputs: tuple[str, ...] = kwargs.pop("output", self._inputs)
        self._outputs = self._tuple_it(self._outputs)

        self._params = kwargs.pop("params", {})
        if not isinstance(self._params, dict):
            raise TypeError("params should be a dict")

        fields: tuple[str, ...] = kwargs.pop("fields", tuple())
        fields = self._tuple_it(fields)

        self._inputs = fields if fields and not self._inputs else self._inputs
        self._outputs = (
            fields if fields and not self._outputs else self._outputs
        )

        local_map = kwargs.pop("map", {})
        self._switch: dict[str, tuple[str, str]] = kwargs.pop("switch", {})

        if not self._inputs and not self._outputs:
            if local_map:
                items = list(local_map.items())
                self._inputs = tuple(x for x, _ in items)
                self._outputs = tuple(x for _, x in items)
            elif self._switch:
                self._inputs = tuple([k for k in self._switch])
                self._outputs = tuple(self._switch[self._inputs[0]])
            else:
                raise ValueError(
                    "Either input and output, fields, or map should be"
                    " provided to Transform constructor."
                )

        # transform image, i.e. the vertex of interest
        # it is used to disambiguate the transformation - vertex relation
        # consider vertices va : {name} and vb: {name}
        # transformations: t_a: {va_name -> name} and t_b : {vb_name -> name}
        self._image: str | None = kwargs.pop("image", None)

    @staticmethod
    def _tuple_it(x):
        if isinstance(x, str):
            x = [x]
        if isinstance(x, list):
            x = tuple(x)
        return x

    @property
    def image(self):
        return self._image

    def _init_foo(self, module_name, class_name, foo_name):
        """
        if module and foo are not None - try to init them
        :param module_name:
        :param class_name:
        :param foo_name:
        :return:
        """
        if module_name is not None:
            try:
                self._module = importlib.import_module(module_name)
            except Exception as e:
                raise TypeError(
                    f"Provided module {module_name} is not valid: {e}"
                )
        elif class_name is not None:
            try:
                self._class = eval(class_name)
            except Exception as e:
                raise Exception(f"Provided class {class_name} not valid: {e}")

        if self._module is not None and foo_name is not None:
            try:
                self._foo = getattr(self._module, foo_name)
            except Exception as e:
                raise ValueError(
                    f"Could not instantiate transform function. Exception: {e}"
                )

    def __call__(self, *nargs, **kwargs):
        """

        :param nargs:
        :param kwargs:
        :return:
        """

        is_mapping = self._foo is None
        return_doc = kwargs.pop("__return_doc", False) or is_mapping

        if is_mapping:
            try:
                input_doc = nargs[0]
                if isinstance(input_doc, dict):
                    output_values = [input_doc[k] for k in self._inputs]
                else:
                    output_values = nargs
            except Exception as e:
                raise ValueError(
                    "For mapping transforms the first argument should be a"
                    f" dict containing all input keys: {e}"
                )
        else:
            if nargs and isinstance(input_doc := nargs[0], dict):
                new_args = [input_doc[k] for k in self.input]
                output_values = self._foo(*new_args, **kwargs, **self._params)
            else:
                output_values = self._foo(*nargs, **kwargs, **self._params)
        if return_doc:
            r = self._dress_as_dict(output_values)
        else:
            r = output_values
        return r

    def _dress_as_dict(self, transform_result):
        if isinstance(transform_result, Iterable):
            upd = {k: v for k, v in zip(self._outputs, transform_result)}
        else:
            upd = {self._outputs[-1]: transform_result}
        for k0, (q, qq) in self._switch.items():
            item = upd.pop(k0, None)
            if item is not None:
                upd.update({q: k0, qq: item})
        return upd

    @property
    def input(self):
        return self._inputs

    @property
    def output(self) -> tuple[str, ...]:
        return self._outputs

    def __str__(self):
        return f"{id(self)} | {self._foo} {self._inputs} -> {self._outputs}"

    __repr__ = __str__
