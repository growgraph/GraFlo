from collections import defaultdict


def update_defaultdict(dd_a: defaultdict, dd_b: defaultdict):
    for k, v in dd_b.items():
        dd_a[k] += v
    return dd_a
