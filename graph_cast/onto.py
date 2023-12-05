import dataclasses
from abc import ABCMeta, abstractmethod
from enum import Enum, EnumMeta
from types import MappingProxyType

from dataclass_wizard import JSONWizard
from dataclass_wizard.enums import DateTimeTo


class MetaEnum(EnumMeta):
    def __contains__(cls, item):
        try:
            cls(item)
        except ValueError:
            return False
        return True


class BaseEnum(Enum, metaclass=MetaEnum):
    pass


class InputType(str, BaseEnum):
    JSON = "json"
    TABLE = "table"


class DBFlavor(str, BaseEnum):
    ARANGO = "arango"
    NEO4J = "neo4j"


class ComparisonOperator(str, BaseEnum):
    NEQ = "!="
    EQ = "=="
    GE = ">="
    LE = "<="
    GT = ">"
    LT = "<"
    IN = "IN"


class LogicalOperator(str, BaseEnum):
    AND = "AND"
    OR = "OR"
    NOT = "NOT"


class BaseDataclass(JSONWizard, JSONWizard.Meta):
    marshal_date_time_as = DateTimeTo.ISO_FORMAT
    key_transform_with_dump = "SNAKE"
    # skip_defaults = True


@dataclasses.dataclass
class AbsClause(BaseDataclass, metaclass=ABCMeta):
    pass

    @abstractmethod
    def cast_filter(self, doc_name):
        pass


@dataclasses.dataclass
class LeafClause(AbsClause):
    cmp_operator: ComparisonOperator
    const: list[str]
    field: str | None = None
    operator: str | None = None

    def __post_init__(self):
        if isinstance(self.const, str):
            self.const = [self.const]

    def cast_filter(self, doc_name="doc", kind: DBFlavor = DBFlavor.ARANGO):
        if kind == DBFlavor.ARANGO:
            return self._cast_arango(doc_name)
        else:
            raise ValueError(f"kind {kind} not implemented")

    def _cast_arango(self, doc_name):
        const = f"{self.const[0]}" if len(self.const) == 1 else f"{self.const}"
        lemma = f"{self.cmp_operator} {const}"
        if self.operator is not None:
            lemma = f"{self.operator} {lemma}"
        if self.field is not None:
            lemma = f'{doc_name}["{self.field}"] {lemma}'
        return lemma


@dataclasses.dataclass
class Clause(AbsClause):
    operator: LogicalOperator
    deps: list[AbsClause]

    def cast_filter(self, doc_name="doc", kind: DBFlavor = DBFlavor.ARANGO):
        if kind == DBFlavor.ARANGO:
            return self._cast_arango(doc_name)

    def _cast_arango(self, doc_name):
        if len(self.deps) == 1:
            if self.operator == LogicalOperator.NOT:
                return f"{self.operator} {self.deps[0].cast_filter(doc_name)}"
            else:
                raise ValueError(
                    f" length if deps {len(self.deps)} is but operator is not"
                    f" {LogicalOperator.NOT}"
                )
        else:
            return f" {self.operator} ".join(
                [item.cast_filter(doc_name) for item in self.deps]
            )


def init_filter(current: list | dict):
    if isinstance(current, list):
        if current[0] in ComparisonOperator:
            return LeafClause(*current)
        elif current[0] in LogicalOperator:
            return Clause(*current)
    elif isinstance(current, dict):
        k = list(current.keys())[0]
        clauses = [init_filter(v) for v in current[k]]
        return Clause(operator=k, deps=clauses)


InputTypeFileExtensions = MappingProxyType(
    {InputType.JSON: (InputType.JSON,), InputType.TABLE: ("csv",)}
)
