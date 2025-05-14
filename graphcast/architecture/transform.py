from __future__ import annotations

import dataclasses
import importlib
import logging
from copy import deepcopy
from typing import Optional

from graphcast.onto import BaseDataclass

logger = logging.getLogger(__name__)


class TransformException(BaseException):
    pass


@dataclasses.dataclass
class ProtoTransform(BaseDataclass):
    name: Optional[str] = None
    module: Optional[str] = None
    params: dict = dataclasses.field(default_factory=dict)
    foo: Optional[str] = None
    input: tuple[str, ...] = dataclasses.field(default_factory=tuple)
    output: tuple[str, ...] = dataclasses.field(default_factory=tuple)

    def __post_init__(self):
        self._foo = None
        self._init_foo()

        self.input = self._tuple_it(self.input)

        if not self.output:
            self.output = self.input
        self.output = self._tuple_it(self.output)

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
                raise TypeError(f"Provided module {self.module} is not valid: {e}")
            try:
                self._foo = getattr(_module, self.foo)
            except Exception as e:
                raise ValueError(
                    f"Could not instantiate transform function. Exception: {e}"
                )

    def __lt__(self, other):
        if self._foo is None and other._foo is not None:
            return True
        return False


@dataclasses.dataclass
class Transform(ProtoTransform):
    fields: tuple[str, ...] = dataclasses.field(default_factory=tuple)
    map: dict[str, str] = dataclasses.field(default_factory=dict)
    switch: dict[str, str] = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        super().__post_init__()
        self.functional_transform = False
        if self._foo is not None:
            self.functional_transform = True

        self.input = self._tuple_it(self.input)

        self.fields = self._tuple_it(self.fields)

        self.input = self.fields if self.fields and not self.input else self.input
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
            elif not self.name:
                raise ValueError(
                    "Either input and output, fields, map or name should be"
                    " provided in Transform constructor."
                )

    def __call__(self, *nargs, **kwargs):
        """

        :param nargs:
        :param kwargs:
        :return:
        """

        is_mapping = self._foo is None

        if is_mapping:
            input_doc = nargs[0]
            if isinstance(input_doc, dict):
                output_values = [input_doc[k] for k in self.input]
            else:
                output_values = nargs
        else:
            if nargs and isinstance(input_doc := nargs[0], dict):
                new_args = [input_doc[k] for k in self.input]
                output_values = self._foo(*new_args, **kwargs, **self.params)
            else:
                output_values = self._foo(*nargs, **kwargs, **self.params)

        if self.output:
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

    @property
    def is_dummy(self):
        return (self.name is not None) and (not self.map and self._foo is None)

    def update(self, t: Transform):
        t_copy = deepcopy(t)
        if self.input:
            t_copy.input = self.input
        if self.output:
            t_copy.output = self.output
        if self.params:
            t_copy.params.update(self.params)
        t_copy.__post_init__()
        return t_copy

    # def __str__(self):
    #     return f"{id(self)} | {self.foo}\n{self.input} -> {self.output}"
    #
    # __repr__ = __str__

    def get_barebone(
        self, other: Optional[Transform]
    ) -> tuple[Optional[Transform], Optional[Transform]]:
        """

        Args:
            other:

        Returns:
            tuple[updated self transform, transform to store in lib]
        """

        self_param = self.to_dict(skip_defaults=True)
        if self.foo is not None:
            # self will be the lib transform
            return None, self
        elif other is not None and other.foo is not None:
            # init self from other
            self_param.pop("foo", None)
            self_param.pop("module", None)
            other_param = other.to_dict(skip_defaults=True)
            other_param.update(self_param)
            return Transform(**other_param), None
        else:
            return None, None
