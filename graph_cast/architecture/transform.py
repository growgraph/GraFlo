import dataclasses
import importlib
import logging

from graph_cast.onto import BaseDataclass

logger = logging.getLogger(__name__)


class TransformException(BaseException):
    pass


@dataclasses.dataclass
class Transform(BaseDataclass):
    module: str | None = None
    class_name: str | None = None
    foo: str | None = None
    fields: tuple[str, ...] = dataclasses.field(default_factory=tuple)
    input: tuple[str, ...] = dataclasses.field(default_factory=tuple)
    output: tuple[str, ...] = dataclasses.field(default_factory=tuple)
    params: dict = dataclasses.field(default_factory=dict)
    map: dict[str, str] = dataclasses.field(default_factory=dict)
    switch: dict[str, str] = dataclasses.field(default_factory=dict)

    """
        transform image, i.e. the vertex of interest
        it is used to disambiguate the transformation - vertex relation
        consider vertices va : {name} and vb: {name}
        transformations: t_a: {va_name -> name} and t_b : {vb_name -> name}
    """
    image: str | None = None

    def __post_init__(self):
        self._foo = None

        self._init_foo()
        self.functional_transform = False
        if self._foo is not None:
            self.functional_transform = True

        self.input = self._tuple_it(self.input)

        self.fields = self._tuple_it(self.fields)

        self.input = (
            self.fields if self.fields and not self.input else self.input
        )
        if not self.output:
            self.output = self.input
        self.output = self._tuple_it(self.output)

        if not self.input and not self.output:
            if self.map:
                items = list(self.map.items())
                self.input = tuple(x for x, _ in items)
                self.output = tuple(x for _, x in items)
            elif self.switch:
                self.input = tuple([k for k in self.switch])
                self.output = tuple(self.switch[self.input[0]])
            else:
                raise ValueError(
                    "Either input and output, fields, or map should be"
                    " provided to Transform constructor."
                )

    @staticmethod
    def _tuple_it(x):
        if isinstance(x, str):
            x = [x]
        if isinstance(x, list):
            x = tuple(x)
        return x

    def _init_foo(self):
        """
        if module and foo are not None - try to init them
        :return:
        """
        if self.module is not None:
            try:
                _module = importlib.import_module(self.module)
            except Exception as e:
                raise TypeError(
                    f"Provided module {self.module} is not valid: {e}"
                )
            try:
                self._foo = getattr(_module, self.foo)
            except Exception as e:
                raise ValueError(
                    f"Could not instantiate transform function. Exception: {e}"
                )

        elif self.class_name is not None:
            try:
                eval(self.class_name)
            except Exception as e:
                raise Exception(
                    f"Provided class {self.class_name} not valid: {e}"
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
                    output_values = [input_doc[k] for k in self.input]
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
                output_values = self._foo(*new_args, **kwargs, **self.params)
            else:
                output_values = self._foo(*nargs, **kwargs, **self.params)
        if return_doc:
            r = self._dress_as_dict(output_values)
        else:
            r = output_values
        return r

    def _dress_as_dict(self, transform_result):
        if isinstance(transform_result, (list, tuple)) and not self.switch:
            upd = {k: v for k, v in zip(self.output, transform_result)}
        else:
            # TODO : temporary solution works only there is one switch clause
            upd = {self.output[-1]: transform_result}
        for k0, (q, qq) in self.switch.items():
            upd.update({q: k0})
        return upd

    def __str__(self):
        return f"{id(self)} | {self.foo} {self.input} -> {self.output}"

    __repr__ = __str__
