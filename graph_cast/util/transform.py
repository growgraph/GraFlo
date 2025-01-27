import json
import logging
import re
import time
from collections import defaultdict
from datetime import datetime

ORDINAL_SUFFIX = ["st", "nd", "rd", "th"]

logger = logging.getLogger(__name__)


def standardize(k):
    """
    Standardizes a string key by removing periods and splitting.

    Handles comma and space-separated strings, normalizing their format.

    Args:
        k (str): Input string to be standardized.

    Returns:
        str: Cleaned and standardized string.

    Example:
        "John. Doe, Smith" -> "John,Doe,Smith"
    """

    k = k.translate(str.maketrans({".": ""}))
    # try to split by ", "
    k = k.split(", ")
    if len(k) < 2:
        k = k[0].split(" ")
    else:
        k[1] = k[1].translate(str.maketrans({" ": ""}))
    return ",".join(k)


def parse_date_standard(input_str):
    dt = datetime.strptime(input_str, "%Y-%m-%d")
    return dt.year, dt.month, dt.day


def parse_date_conf(input_str):
    dt = datetime.strptime(input_str, "%Y%m%d")
    return dt.year, dt.month, dt.day


def parse_date_ibes(date0, time0):
    """
    Converts IBES date and time to ISO 8601 format datetime.

    Args:
        date0 (str/int): Date in YYYYMMDD format.
        time0 (str): Time in HH:MM:SS format.

    Returns:
        str: Datetime in ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ).

    Example:
        parse_date_ibes(20160126, "9:35:52")
        -> "2016-01-26T09:35:52Z"
    """
    date0 = str(date0)
    year, month, day = date0[:4], date0[4:6], date0[6:]
    full_datetime = f"{year}-{month}-{day}T{time0}Z"

    return full_datetime


def parse_date_yahoo(date0):
    """

    :param date0: as "2016-01-26"
    :return: datetime as "2016-01-26T14:19:09.522"
    """
    full_datetime = f"{date0}T12:00:00Z"
    # dt = datetime.strptime(date0, "%Y-%m-%d").timetuple()

    # timestamp = time.mktime(dt)
    # return (timestamp,)
    return full_datetime


def round_str(x, **kwargs):
    return round(float(x), **kwargs)


def parse_date_standard_to_epoch(input_str):
    dt = datetime.strptime(input_str, "%Y-%m-%d").timetuple()
    timestamp = time.mktime(dt)
    return timestamp


def cast_ibes_analyst(s):
    """
    Splits and normalizes analyst name strings.

    Handles various name formats like 'ADKINS/NARRA' or 'ARFSTROM      J'.

    Args:
        s (str): Analyst name string.

    Returns:
        tuple: (last_name, first_initial)

    Examples:
        'ADKINS/NARRA' -> ('ADKINS', 'N')
        'ARFSTROM      J' -> ('ARFSTROM', 'J')
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
    return _parse_date_reference(input_str)["year"]


def _parse_date_reference(input_str):
    """
    Parses complex, human-written date references.

    Handles various date formats like:
    - "1923, May 10"
    - "1923, July"
    - "1921, Sept"
    - "1935-36"
    - "1926, December 24th"

    Args:
        input_str (str): Date string in various formats.

    Returns:
        dict: Parsed date information with keys 'year', optional 'month', 'day'.
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
    """
    Removes None values from dictionaries, with optional key exceptions.

    Args:
        docs (list): List of dictionaries to clean.
        keys_keep_nones (list, optional): Keys to keep even if their value is None.

    Returns:
        list: Cleaned list of dictionaries.
    """
    docs = [
        {k: v for k, v in tdict.items() if v or k in keys_keep_nones} for tdict in docs
    ]
    return docs


def parse_multi_item(s, mapper: dict, direct: list):
    """
    Parses complex multi-item strings into structured data.

    Supports parsing strings with quoted or bracketed items.

    Args:
        s (str): Input string to parse.
        mapper (dict): Mapping of input keys to output keys.
        direct (list): Direct keys to extract.

    Returns:
        defaultdict: Parsed items with lists as values.
    """
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
    """
    Removes duplicate dictionaries from a list.

    Uses JSON serialization to identify unique dictionaries.

    Args:
        docs (list): List of dictionaries.

    Returns:
        list: List of unique dictionaries.
    """

    docs = {json.dumps(d, sort_keys=True) for d in docs}
    docs = [json.loads(t) for t in docs]
    return docs


def split_keep_part(s: str, sep="/", keep=-1) -> str:
    if isinstance(keep, list):
        items = s.split(sep)
        return sep.join(items[k] for k in keep)
    else:
        return s.split(sep)[keep]
