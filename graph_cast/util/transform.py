import json
import logging
import re
import time
from collections import defaultdict
from datetime import datetime

ORDINAL_SUFFIX = ["st", "nd", "rd", "th"]

logger = logging.getLogger(__name__)


def standardize(k):
    # 1. clean period
    k = k.translate(str.maketrans({".": ""}))
    # 2. try to split by ", "
    k = k.split(", ")
    if len(k) < 2:
        k = k[0].split(" ")
    else:
        k[1] = k[1].translate(str.maketrans({" ": ""}))
    return ",".join(k)


def parse_date_standard(input_str):
    dt = datetime.strptime(input_str, "%Y-%m-%d")
    year, month, day = dt.year, dt.month, dt.day
    return year, month, day


def parse_date_conf(input_str):
    dt = datetime.strptime(input_str, "%Y%m%d")
    year, month, day = dt.year, dt.month, dt.day
    return year, month, day


def parse_date_ibes(date0, time0):
    """

    :param date0: as "20160126"
    :param time0: as "9:35:52"
    :return: datetime as "2013-01-15T14:19:09.522"
    """
    year, month, day = date0[:4], date0[4:6], date0[6:]
    full_datetime = f"{year}-{month}-{day}T{time0}Z"

    # full_datetime = f"{year}-{month}-{day}T{time0}"
    # dt = datetime.strptime(full_datetime, "%Y-%m-%dT%H:%M:%SZ").timetuple()
    # timestamp = time.mktime(dt)
    # return (timestamp,)
    return (full_datetime,)


def parse_date_yahoo(date0):
    """

    :param date0: as "2016-01-26"
    :return: datetime as "2016-01-26T14:19:09.522"
    """
    full_datetime = f"{date0}T12:00:00Z"
    # dt = datetime.strptime(date0, "%Y-%m-%d").timetuple()

    # timestamp = time.mktime(dt)
    # return (timestamp,)
    return (full_datetime,)


def round_str(x, **kwargs):
    return (round(float(x), **kwargs),)


def parse_date_standard_to_epoch(input_str):
    dt = datetime.strptime(input_str, "%Y-%m-%d").timetuple()
    timestamp = time.mktime(dt)
    return (timestamp,)


def cast_ibes_analyst(s):
    """
        split string like 'ADKINS/NARRA' and 'ARFSTROM      J'

    :param s:
    :return:
    """
    if " " in s or "\t" in s:
        r = s.split()[:2]
        if len(r) < 2:
            return r[0], ""
        else:
            return r[0], r[1][:1]
    else:
        r = s.split("/")
        if s.startswith("/"):
            r = r[1:3]
        else:
            r = r[:2]
        if len(r) < 2:
            return r[0], ""
        else:
            return r[0], r[1][:1]


def parse_date_reference(input_str):
    return (_parse_date_reference(input_str)["year"],)


def _parse_date_reference(input_str):
    """
    examples:

    "year" : "1923, May 10"
    "year" : "1923, July"
    "year" : "1921, Sept"
    "year" : "1935-36"
    "year" : "1926, December 24th"
    "year" : "1923, May 10"
    "year" : "undated"
    :param input_str:
    :return:
    """
    if "," in input_str:
        if len(input_str.split(" ")) == 3:
            if input_str[-2:] in ORDINAL_SUFFIX:
                input_str = input_str[:-2]
            try:
                dt = datetime.strptime(input_str, "%Y, %B %d")
                return {"year": dt.year, "month": dt.month, "day": dt.day}
            except:
                try:
                    aux = input_str.split(" ")
                    input_str = " ".join([aux[0]] + [aux[1][:3]] + [aux[2]])
                    dt = datetime.strptime(input_str, "%Y, %b %d")
                    return {"year": dt.year, "month": dt.month, "day": dt.day}
                except:
                    return {"year": input_str}
        else:
            try:
                dt = datetime.strptime(input_str, "%Y, %B")
                return {"year": dt.year, "month": dt.month}
            except:
                try:
                    aux = input_str.split(" ")
                    input_str = " ".join([aux[0]] + [aux[1][:3]])
                    dt = datetime.strptime(input_str, "%Y, %b")
                    return {"year": dt.year, "month": dt.month}
                except:
                    return {"year": input_str}
    else:
        try:
            dt = datetime.strptime(input_str[:4], "%Y")
            return {"year": dt.year}
        except:
            return {"year": input_str}


def try_int(x):
    try:
        x = int(x)
        return x
    except:
        return x


def clear_first_level_nones(docs, keys_keep_nones=None):
    docs = [
        {k: v for k, v in tdict.items() if v or k in keys_keep_nones}
        for tdict in docs
    ]
    return docs


def parse_multi_item(s, mapper: dict, direct: list):
    if "'" in s:
        items_str = re.findall(r"\"(.*?)\"", s) + re.findall(r"\'(.*?)\'", s)
    else:
        # remove brackets
        items_str = re.findall(r"\[([^]]+)", s)[0].split()
    r: defaultdict[str, list] = defaultdict(list)
    for item in items_str:
        doc0 = [ss.strip().split(":") for ss in item.split(",")]
        if all([len(x) == 2 for x in doc0]):
            doc0_dict = dict(doc0)
            for n_init, n_final in mapper.items():
                try:
                    r[n_final] += [doc0_dict[n_init]]
                except KeyError:
                    r[n_final] += [None]

            for n_final in direct:
                try:
                    r[n_final] += [doc0_dict[n_final]]
                except KeyError:
                    r[n_final] += [None]
        else:
            for key, value in zip(direct, doc0):
                r[key] += [value]

    return r


def pick_unique_dict(docs):
    docs = {json.dumps(d, sort_keys=True) for d in docs}
    docs = [json.loads(t) for t in docs]
    return docs


def merge_doc_basis(docs, keys):
    """

    :param docs:
    :param keys:
    :return:
    """

    # represent each doc as a sorted tuple keeping only keys from keys
    flat_rep = [
        tuple(sorted({k: v for k, v in item.items() if k in keys}.items()))
        for item in docs
    ]

    # take only unique tuples
    qdict = {q: dict() for q in set(flat_rep)}

    for item in docs:
        q = tuple(sorted({k: v for k, v in item.items() if k in keys}.items()))
        qdict[q].update(item)
    return list(qdict.values())
