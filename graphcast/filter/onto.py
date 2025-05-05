import dataclasses
import logging
from abc import ABCMeta, abstractmethod
from types import MappingProxyType

from graphcast.onto import BaseDataclass, BaseEnum, ExpressionFlavor

logger = logging.getLogger(__name__)


class LogicalOperator(BaseEnum):
    AND = "AND"
    OR = "OR"
    NOT = "NOT"
    IMPLICATION = "IF_THEN"


def implication(ops):
    a, b = ops
    return b if a else True


OperatorMapping = MappingProxyType(
    {
        LogicalOperator.AND: all,
        LogicalOperator.OR: any,
        LogicalOperator.IMPLICATION: implication,
    }
)


class ComparisonOperator(BaseEnum):
    NEQ = "!="
    EQ = "=="
    GE = ">="
    LE = "<="
    GT = ">"
    LT = "<"
    IN = "IN"


@dataclasses.dataclass
class AbsClause(BaseDataclass, metaclass=ABCMeta):
    pass

    @abstractmethod
    def __call__(
        self,
        doc_name,
        kind: ExpressionFlavor = ExpressionFlavor.ARANGO,
        **kwargs,
    ):
        pass


@dataclasses.dataclass
class LeafClause(AbsClause):
    """
    doc_name["field"] operator cmp_op value

    e.g. for arango:  `doc_name["year"] % 2 == 0`

    e.g. for python:

    operator(doc) cmp_op value



    """

    cmp_operator: ComparisonOperator | None = None
    value: list = dataclasses.field(default_factory=list)
    field: str | None = None
    operator: str | None = None

    def __post_init__(self):
        if not isinstance(self.value, list):
            self.value = [self.value]

    def __call__(
        self,
        doc_name="doc",
        kind: ExpressionFlavor = ExpressionFlavor.ARANGO,
        **kwargs,
    ):
        if not self.value:
            logger.warning(f"for {self} value is not set : {self.value}")
        if kind == ExpressionFlavor.ARANGO:
            assert self.cmp_operator is not None
            return self._cast_arango(doc_name)
        elif kind == ExpressionFlavor.NEO4J:
            assert self.cmp_operator is not None
            return self._cast_cypher(doc_name)
        elif kind == ExpressionFlavor.PYTHON:
            return self._cast_python(**kwargs)
        else:
            raise ValueError(f"kind {kind} not implemented")

    def _cast_value(self):
        value = f"{self.value[0]}" if len(self.value) == 1 else f"{self.value}"
        if len(self.value) == 1:
            if isinstance(self.value[0], str):
                value = f'"{self.value[0]}"'
            elif self.value[0] is None:
                value = "null"
            else:
                value = f"{self.value[0]}"
        return value

    def _cast_arango(self, doc_name):
        const = self._cast_value()

        lemma = f"{self.cmp_operator} {const}"
        if self.operator is not None:
            lemma = f"{self.operator} {lemma}"

        if self.field is not None:
            lemma = f'{doc_name}["{self.field}"] {lemma}'
        return lemma

    def _cast_cypher(self, doc_name):
        const = self._cast_value()
        if self.cmp_operator == ComparisonOperator.EQ:
            cmp_operator = "="
        else:
            cmp_operator = self.cmp_operator
        lemma = f"{cmp_operator} {const}"
        if self.operator is not None:
            lemma = f"{self.operator} {lemma}"

        if self.field is not None:
            lemma = f"{doc_name}.{self.field} {lemma}"
        return lemma

    def _cast_python(self, **kwargs):
        field = kwargs.pop(self.field, None)
        if field is not None:
            foo = getattr(field, self.operator)
            return foo(self.value[0])
        else:
            return False


@dataclasses.dataclass
class Clause(AbsClause):
    operator: LogicalOperator
    deps: list[AbsClause]

    def __call__(
        self,
        doc_name="doc",
        kind: ExpressionFlavor = ExpressionFlavor.ARANGO,
        **kwargs,
    ):
        if kind == ExpressionFlavor.ARANGO or kind == ExpressionFlavor.ARANGO:
            return self._cast_generic(doc_name=doc_name, kind=kind)
        elif kind == ExpressionFlavor.PYTHON:
            return self._cast_python(kind=kind, **kwargs)

    def _cast_generic(self, doc_name, kind):
        if len(self.deps) == 1:
            if self.operator == LogicalOperator.NOT:
                return f"{self.operator} {self.deps[0](kind=kind, doc_name=doc_name)}"
            else:
                raise ValueError(
                    f" length of deps = {len(self.deps)} but operator is not"
                    f" {LogicalOperator.NOT}"
                )
        else:
            return f" {self.operator} ".join(
                [item(kind=kind, doc_name=doc_name) for item in self.deps]
            )

    def _cast_python(self, kind, **kwargs):
        if len(self.deps) == 1:
            if self.operator == LogicalOperator.NOT:
                return not self.deps[0](kind=kind, **kwargs)
            else:
                raise ValueError(
                    f" length of deps = {len(self.deps)} but operator is not"
                    f" {LogicalOperator.NOT}"
                )
        else:
            return OperatorMapping[self.operator](
                [item(kind=kind, **kwargs) for item in self.deps]
            )


@dataclasses.dataclass
class Expression(AbsClause):
    @classmethod
    def from_dict(cls, current):
        if isinstance(current, list):
            if current[0] in ComparisonOperator:
                return LeafClause(*current)
            elif current[0] in LogicalOperator:
                return Clause(*current)
        elif isinstance(current, dict):
            k = list(current.keys())[0]
            if k in LogicalOperator:
                clauses = [cls.from_dict(v) for v in current[k]]
                return Clause(operator=k, deps=clauses)
            else:
                return LeafClause(**current)

    def __call__(
        self,
        doc_name="doc",
        kind: ExpressionFlavor = ExpressionFlavor.ARANGO,
        **kwargs,
    ):
        pass
