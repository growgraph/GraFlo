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
        if not self._inputs or not self._outputs:
            if local_map:
                items = list(local_map.items())
                self._inputs = tuple(x for x, _ in items)
                self._outputs = tuple(x for _, x in items)
            else:
                raise ValueError(
                    "Either input and output, fields, or map should be"
                    " provided to Transform constructor."
                )

        self._switch: dict[str, tuple[str, str]] = kwargs.pop("switch", {})

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
            upd = {self._outputs[0]: transform_result}
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


# class TableMapper(ATransform):
#     def __init__(self, **kwargs):
#         self._filename = None
#         self._filename_field = None
#         self._request_filename = False
#         if "map" in kwargs:
#             self._raw_map = kwargs["map"]
#         else:
#             self._raw_map = {}
#         self._process_maps()
#         self._check_map_splitter()
#         self._possible_keys = set(self._map) | set(self._map_splitter)
#         if "_filename" in self._map:
#             if "filename" in kwargs:
#                 self._filename = kwargs["filename"]
#             self._request_filename = True
#             self._filename_field = self._map["_filename"]
#             del self._map["_filename"]
#
#     def _process_maps(self):
#         self._map = {
#             k: v for k, v in self._raw_map.items() if isinstance(v, str)
#         }
#         self._map_splitter = {
#             k: v for k, v in self._raw_map.items() if not isinstance(v, str)
#         }
#
#     def _check_map_splitter(self):
#         for k, item in self._map_splitter.items():
#             if not isinstance(item, dict):
#                 raise TypeError(
#                     f" self._raw_map should be a dict : {self._raw_map}"
#                 )
#             if "key" not in item:
#                 raise KeyError(
#                     f" item should contain 'key' and 'value' : {item}"
#                 )
#             if "value" not in item:
#                 item["value"] = "value"
#
#     def _update_filename(self, filename):
#         self._filename = filename
#
#     def update(self, **kwargs):
#         for k, v in kwargs.items():
#             if self._request_filename and k == "filename":
#                 setattr(self, f"_{k}", v.split("/")[-1].split(".")[0])
#
#     @property
#     def input(self) -> list:
#         return list(self._map.keys())
#
#     @property
#     def output(self) -> list:
#         return list(self._map.values())
#
#     def input_split(self):
#         return len(self._map_splitter) > 0
#
#     @property
#     def dynamic_transformations(self):
#         return self._request_filename
#
#     @property
#     def active(self):
#         return self.input or self.input_split or self.dynamic_transformations
#
#     def __call__(self, item, filename=None) -> dict:
#         acc = {v: item[k] for k, v in self._map.items()}
#         acc.update(
#             {
#                 f"_status@{k}": item[f"_status@{k}"]
#                 for k in self._map
#                 if f"_status@{k}" in item
#             }
#         )
#         # if self._request_filename and filename is not None:
#         #     self._update_filename(filename)
#         if self._request_filename and self._filename_field:
#             acc[self._filename_field] = self._filename
#         for k, cmap in self._map_splitter.items():
#             cm_key, cm_value = cmap["key"], cmap["value"]
#             acc[cm_key] = k
#             acc[cm_value] = item[k]
#             if f"_status@{k}" in item:
#                 acc.update({f"_status@{cm_value}": item[f"_status@{k}"]})
#         return acc
#
#     def __str__(self):
#         return f"{id(self)}:  {self._map} : {self._map_splitter}"
#
#     __repr__ = __str__
