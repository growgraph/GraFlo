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


def strip_prefix(dictlike, prefix="~"):
    new_dictlike = {}
    if isinstance(dictlike, dict):
        for k, v in dictlike.items():
            if isinstance(k, str):
                k = k.lstrip(prefix)
            new_dictlike[k] = strip_prefix(v, prefix)
    elif isinstance(dictlike, list):
        return [strip_prefix(x) for x in dictlike]
    return dictlike
