from __future__ import annotations

import dataclasses

from strenum import StrEnum


class Operator(StrEnum):
    AND = "and"
    OR = "or"
    IMPLICATION = "if_then"


def implication(ops):
    a, b = ops
    return b if a else True


op_map = {
    Operator.AND: all,
    Operator.OR: any,
    Operator.IMPLICATION: implication,
}


class UnitOperand:
    def __init__(self, **kwargs):
        # field, foo, value = None
        self.field = kwargs.pop("field", None)
        self.value = kwargs.pop("value", None)
        self.foo = kwargs.pop("foo", None)

    def __call__(self, **kwargs):
        field = kwargs.pop(self.field, None)
        if field is not None:
            foo = getattr(field, self.foo)
            return foo(self.value)
        else:
            return False

    def __str__(self):
        return (
            f"{self.__class__} | field: {self.field} value: {self.value} ->"
            f" foo: {self.foo}"
        )

    __repr__ = __str__


TypeOp = Operator | UnitOperand


@dataclasses.dataclass
class Node:
    content: TypeOp
    children: list = dataclasses.field(default_factory=lambda: [])


class Filter:
    def __init__(self, dictlike):
        self.root = None
        self._parse_init(dictlike, None)

    def _parse_init(self, dictlike, cnode: Node | None):
        if isinstance(dictlike, list):
            # a list of UnitOperands
            for item in dictlike:
                self._parse_init(item, cnode)

        elif isinstance(dictlike, dict):
            if any([k in list(Operator) for k in dictlike.keys()]):
                keys = [k for k in dictlike.keys()]
                cnode_new = Node(content=Operator(keys[0]))
                if self.root is None:
                    self.root = cnode_new
                if cnode is not None:
                    cnode.children += [cnode_new]
                for item in dictlike.values():
                    self._parse_init(item, cnode_new)
            else:
                try:
                    uo = UnitOperand(**dictlike)
                except Exception as e:
                    raise ValueError(f"Not a valid UnitOperand {e}")
                if self.root is None:
                    self.root = Node(content=uo)
                if cnode is not None:
                    cnode.children += [Node(content=uo)]

    def _parse(self, node: Node, doc) -> bool:
        if isinstance(node.content, Operator):
            return op_map[node.content](
                [self._parse(c, doc) for c in node.children]
            )
        elif isinstance(node.content, UnitOperand):
            return node.content(**doc)
        else:
            return False

    def __call__(self, doc):
        return self._parse(self.root, doc)
