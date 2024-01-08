def sorted_dicts(d):
    if isinstance(d, (tuple, list)):
        if d and all(
            [not isinstance(dd, (list, tuple, dict)) for dd in d[0].values()]
        ):
            return sorted(d, key=lambda x: tuple(x.items()))
    elif isinstance(d, dict):
        return {
            k: v if not isinstance(v, (list, tuple, dict)) else sorted_dicts(v)
            for k, v in d.items()
        }

    return d
