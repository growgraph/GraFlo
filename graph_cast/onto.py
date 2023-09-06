from enum import Enum
from types import MappingProxyType


class InputType(str, Enum):
    JSON = "json"
    TABLE = "table"


class DBFlavor(str, Enum):
    ARANGO = "arango"
    NEO4J = "neo4j"


InputTypeFileExtensions = MappingProxyType(
    {InputType.JSON: (InputType.JSON,), InputType.TABLE: ("csv",)}
)
