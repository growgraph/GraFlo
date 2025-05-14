import dataclasses
from copy import deepcopy
from enum import EnumMeta, StrEnum

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


@dataclasses.dataclass
class BaseDataclass(JSONWizard, JSONWizard.Meta):
    marshal_date_time_as = DateTimeTo.ISO_FORMAT
    key_transform_with_dump = "SNAKE"
    # skip_defaults = True

    def update(self, other):
        if not isinstance(other, type(self)):
            raise TypeError(
                f"Expected {type(self).__name__} instance, got {type(other).__name__}"
            )

        for field in dataclasses.fields(self):
            name = field.name
            current_value = getattr(self, name)
            other_value = getattr(other, name)

            if other_value is None:
                pass
            elif isinstance(other_value, set):
                setattr(self, name, current_value | deepcopy(other_value))
            elif isinstance(other_value, list):
                setattr(self, name, current_value + deepcopy(other_value))
            elif isinstance(other_value, dict):
                setattr(self, name, {**current_value, **deepcopy(other_value)})
            elif dataclasses.is_dataclass(type(other_value)):
                if current_value is not None:
                    current_value.update(other_value)
                else:
                    setattr(self, name, deepcopy(other_value))
            else:
                if current_value is None:
                    setattr(self, name, other_value)

    @classmethod
    def get_fields_members(cls):
        return [k for k in cls.__annotations__ if not k.startswith("_")]
