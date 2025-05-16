from __future__ import annotations

from typing import Union

from graphcast.architecture.onto import GraphEntity


def project_dict(item, keys, how="include"):
    if how == "include":
        return {k: v for k, v in item.items() if k in keys}
    elif how == "exclude":
        return {k: v for k, v in item.items() if k not in keys}
    else:
        return {}


def project_dicts(items, keys, how="include"):
    if how == "include":
        return [{k: v for k, v in item.items() if k in keys} for item in items]
    elif how == "exclude":
        return [{k: v for k, v in item.items() if k not in keys} for item in items]
    else:
        raise ValueError(f" `how` should be exclude or include : instead {how}")


def cast_graph_name_to_triple(s: GraphEntity) -> Union[str, tuple]:
    """Convert a graph name string to a triple format.

    Args:
        s: Graph entity name or ID

    Returns:
        Either a string or tuple representing the graph entity

    Raises:
        ValueError: If the graph name cannot be cast to a valid format
    """
    if isinstance(s, str):
        s2 = s.split("_")
        if len(s2) < 2:
            return s2[0]
        elif len(s2) == 2:
            return *s2[:-1], None
        elif len(s2) == 3:
            if s2[-1] in ["graph", "edges"]:
                return *s2[:-1], None
            else:
                return tuple(s2)
        elif len(s2) == 4 and s2[-1] in ["graph", "edges"]:
            return tuple(s2[:-1])
        raise ValueError(f"Invalid graph_name {s} : can not be cast to GraphEntity")
    else:
        return s
