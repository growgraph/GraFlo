import gzip
import json
import yaml
import pickle
import pandas as pd
import pkgutil
import io
import logging
from os.path import expanduser
from collections.abc import Iterable


logger = logging.getLogger(__name__)


class ResourceHandler:
    @classmethod
    def _find_mode(cls, lemma):
        if lemma in ["yml", "yaml"]:
            return "yaml"
        elif lemma == "json":
            return "json"
        elif lemma in ["pkl", "pickle"]:
            return "pkl"
        elif lemma in ["csv"]:
            return "csv"
        else:
            return None

    @classmethod
    def _dump_pointer(cls, item, p, how, bytes_=True):
        if how == "pkl":
            pickle.dump(item, p, pickle.HIGHEST_PROTOCOL)
        elif how == "yaml":
            yc = yaml.dump(item)
            if bytes_:
                yc = yc.encode("utf-8")
            p.write(yc)
        elif how == "json":
            jc = json.dumps(item, indent=2) + "\n"
            if bytes_:
                jc = jc.encode("utf-8")
            p.write(jc)
        elif how == "csv" and (
            isinstance(item, pd.DataFrame) or isinstance(item, pd.Series)
        ):
            r = item.to_csv()
            if bytes_:
                r = r.encode("utf-8")
            p.write(r)

    @classmethod
    def _open_pointer(cls, p, how, **kwargs):
        if how == "pkl":
            r = pickle.load(p)
        elif how == "yaml":
            r = yaml.load(p, Loader=yaml.FullLoader)
        elif how == "json":
            r = json.load(p)
        elif how == "csv":
            r = pd.read_csv(p, **kwargs)
        else:
            r = dict()
        return r

    @classmethod
    def load(
        cls,
        ppath=None,
        pname=None,
        how="yaml",
        compression=None,
        fpath=None,
        **kwargs,
    ):
        if fpath is None:
            lemmas = pname.split(".")
            if lemmas[-1] == "gz":
                compression = "gz"
                how_ = cls._find_mode(lemmas[-2])
            else:
                how_ = cls._find_mode(lemmas[-1])
            if how_:
                how = how_
            bytes_ = pkgutil.get_data(ppath, pname)
        else:
            fpath = expanduser(fpath)
            lemmas = fpath.split(".")
            if lemmas[-1] == "gz":
                compression = "gz"
                how_ = cls._find_mode(lemmas[-2])
            else:
                how_ = cls._find_mode(lemmas[-1])
            if how_:
                how = how_
            with open(fpath, "rb") as fp:
                bytes_ = fp.read()
        if compression == "gz":
            with gzip.GzipFile(fileobj=io.BytesIO(bytes_), mode="r") as p:
                r = cls._open_pointer(p, how, **kwargs)
        else:
            with io.BytesIO(bytes_) as p:
                r = cls._open_pointer(p, how, **kwargs)
        return r

    @classmethod
    def dump(cls, item, path, how="yaml"):
        """

        :param item:
        :param path: if path ends with ".gz" the output will be gzip compressed
        :param how:
        :return:
        """
        lemmas = path.split(".")
        if lemmas[-1] == "gz":
            compression = "gz"
            how_ = cls._find_mode(lemmas[-2])
        else:
            compression = None
            how_ = cls._find_mode(lemmas[-1])
        if how_:
            how = how_
        if how == "pkl":
            mode = "wb"
        else:
            mode = "w"
        if compression == "gz":
            if not path.endswith(".gz"):
                path += ".gz"
            with gzip.GzipFile(path, mode=mode) as p:
                cls._dump_pointer(item, p, how)
        else:
            with open(path, mode=mode) as p:
                cls._dump_pointer(item, p, how, bytes_=False)


excluded_types = (str, dict)


def equals(a, b):
    if isinstance(a, dict) and isinstance(b, dict):
        if a.keys() != b.keys():
            logger.error(f"a: {a.keys()} ; b: {b.keys()}")
            return False
        else:
            return all([equals(a[k], b[k]) for k in a.keys()])
    elif (isinstance(a, Iterable) and not isinstance(a, excluded_types)) and (
        isinstance(b, Iterable) and not isinstance(b, excluded_types)
    ):
        all([equals(ea, eb) for ea, eb in zip(a, b)])
    else:
        return a == b
