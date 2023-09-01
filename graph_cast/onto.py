from enum import Enum
from types import MappingProxyType


class InputType(str, Enum):
    JSON = "json"
    TABLE = "table"


InputTypeFileExtensions = MappingProxyType(
    {InputType.JSON: (InputType.JSON,), InputType.TABLE: ("csv",)}
)
