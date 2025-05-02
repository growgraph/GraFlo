from __future__ import annotations

from enum import EnumMeta, StrEnum
from types import MappingProxyType

from dataclass_wizard import JSONWizard
from dataclass_wizard.enums import DateTimeTo


class MetaEnum(EnumMeta):
    def __contains__(cls, item, **kwargs):
        try:
            cls(item, **kwargs)
        except ValueError:
            return False
        return True


class BaseEnum(StrEnum, metaclass=MetaEnum):
    pass


class InputType(BaseEnum):
    JSON = "json"
    CSV = "csv"


class ResourceType(BaseEnum):
    ROWLIKE = "row"
    TREELIKE = "tree"


class DBFlavor(BaseEnum):
    ARANGO = "arango"
    NEO4J = "neo4j"


class ExpressionFlavor(BaseEnum):
    ARANGO = "arango"
    NEO4J = "neo4j"
    PYTHON = "python"


class AggregationType(BaseEnum):
    COUNT = "COUNT"
    MAX = "MAX"
    MIN = "MIN"
    AVERAGE = "AVERAGE"
    SORTED_UNIQUE = "SORTED_UNIQUE"


class BaseDataclass(JSONWizard, JSONWizard.Meta):
    marshal_date_time_as = DateTimeTo.ISO_FORMAT
    key_transform_with_dump = "SNAKE"
    # skip_defaults = True


InputTypeFileExtensions = MappingProxyType(
    {
        ResourceType.TREELIKE: (InputType.JSON,),
        ResourceType.ROWLIKE: (InputType.CSV,),
    }
)
