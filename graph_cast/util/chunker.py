from __future__ import annotations

import abc
import csv
import gc
import gzip
import json
import logging
import pathlib
import re
from contextlib import contextmanager
from pathlib import Path
from shutil import copyfileobj
from typing import Any, Callable, TextIO, TypeVar
from xml.etree import ElementTree as et

import ijson
import pandas as pd
import xmltodict

from graph_cast.architecture.onto import BaseEnum, EncodingType

AbstractChunkerType = TypeVar("AbstractChunkerType", bound="AbstractChunker")

logger = logging.getLogger(__name__)


class ChunkerType(str, BaseEnum):
    JSON = "json"
    JSONL = "jsonl"
    TABLE = "table"
    TRIVIAL = "trivial"


class AbstractChunker(abc.ABC):
    def __init__(self, batch_size=10, limit=None):
        self.units_processed = 0
        self.batch_size = batch_size
        self.limit: int | None = limit
        self.cnt = 0
        self.iteration_tried = False

    def _limit_reached(self):
        return self.limit is not None and self.cnt >= self.limit

    def __iter__(self):
        if not self.iteration_tried:
            self._prepare_iteration()
        return self

    def __next__(self):
        batch = self._next_item()
        self.cnt += len(batch)
        if not batch or self._limit_reached():
            raise StopIteration
        return batch

    @abc.abstractmethod
    def _next_item(self):
        pass

    def _prepare_iteration(self):
        self.iteration_tried = True


class FileChunker(AbstractChunker):
    def __init__(
        self,
        filename,
        encoding: EncodingType = EncodingType.UTF_8,
        mode="t",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.filename: Path = filename
        self.file_obj: TextIO | gzip.GzipFile | None = None
        self.encoding: EncodingType | None = encoding
        # b for binary, t for text
        self.mode = mode
        if self.mode == "b":
            self.encoding = None

    def _next_item(self):
        return next(self.file_obj)

    def _prepare_iteration(self):
        super()._prepare_iteration()
        if ".gz" in self.filename.suffixes:
            self.file_obj = gzip.open(
                self.filename.absolute().as_posix(),
                f"r{self.mode}",
                encoding=self.encoding,
            )
        else:
            self.file_obj = open(
                self.filename.absolute().as_posix(),
                f"r{self.mode}",
                encoding=self.encoding,
            )

    def __next__(self):
        batch = []

        if self._limit_reached():
            self.file_obj.close()
            raise StopIteration
        while len(batch) < self.batch_size and not self._limit_reached():
            try:
                batch += [self._next_item()]
                self.cnt += 1
            except StopIteration:
                if batch:
                    return batch
                # eof reached
                self.file_obj.close()
                raise StopIteration

        return batch


class TableChunker(FileChunker):
    def __init__(self, **kwargs):
        self.sep = kwargs.pop("sep", ",")
        super().__init__(**kwargs)
        self.header: list[str]

    def _prepare_iteration(self):
        super()._prepare_iteration()
        header = next(self.file_obj)
        self.header = header.rstrip("\n").split(self.sep)

    def __next__(self):
        lines = super().__next__()
        lines2 = [
            next(csv.reader([line.rstrip()], skipinitialspace=True)) for line in lines
        ]
        dressed = [dict(zip(self.header, row)) for row in lines2]
        return dressed


class JsonlChunker(FileChunker):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __next__(self):
        lines = super().__next__()
        lines2 = [json.loads(line) for line in lines]
        return lines2


class JsonChunker(FileChunker):
    def __init__(self, **kwargs):
        super().__init__(mode="b", **kwargs)
        self.parser: Any

    def _prepare_iteration(self):
        super()._prepare_iteration()
        self.parser = ijson.items(self.file_obj, "item")

    def _next_item(self):
        return next(self.parser)


class TrivialChunker(AbstractChunker):
    def __init__(self, array: list[dict], **kwargs):
        super().__init__(**kwargs)
        self.array = array

    def _next_item(self):
        return self.array[self.cnt : self.cnt + self.batch_size]

    def __next__(self):
        batch = self._next_item()
        self.cnt += len(batch)
        if not batch or self._limit_reached():
            raise StopIteration
        return batch


class ChunkerDataFrame(AbstractChunker):
    def __init__(self, df: pd.DataFrame, **kwargs):
        super().__init__(**kwargs)
        self.df = df
        self.columns = df.columns

    def _next_item(self):
        cid = self.cnt
        pre_batch = self.df.iloc[cid : cid + self.batch_size].values.tolist()
        batch = [{k: v for k, v in zip(self.columns, item)} for item in pre_batch]
        return batch


class ChunkerFactory:
    @classmethod
    def create_chunker(cls, **kwargs) -> AbstractChunker:
        resource: Path | list[dict] | pd.DataFrame | None = kwargs.pop("resource", None)
        chunker_type = kwargs.pop("type", None)
        if isinstance(resource, list):
            return TrivialChunker(array=resource, **kwargs)
        elif isinstance(resource, pd.DataFrame):
            return ChunkerDataFrame(df=resource, **kwargs)
        elif isinstance(resource, Path):
            if chunker_type == ChunkerType.JSONL or (
                chunker_type is None and ".jsonl" in resource.suffixes
            ):
                return JsonlChunker(filename=resource, **kwargs)
            elif chunker_type == ChunkerType.JSON or (
                chunker_type is None and ".json" in resource.suffixes
            ):
                return JsonChunker(filename=resource, **kwargs)
            elif chunker_type == ChunkerType.TABLE or (
                chunker_type is None
                and any([".csv" in resource.suffixes, ".tsv" in resource.suffixes])
            ):
                if ".tsv" in resource.suffixes:
                    return TableChunker(filename=resource, sep="\t", **kwargs)
                else:
                    return TableChunker(filename=resource, sep=",", **kwargs)
        raise ValueError(
            "Could not determine the type of required Chunker "
            f"for type={chunker_type} and resource={resource}"
        )


class ChunkFlusherMono:
    def __init__(self, target_prefix, chunksize, maxchunks=None, suffix=None):
        self.target_prefix = target_prefix
        self.acc = []
        self.chunk_count = 0
        self.chunksize = chunksize
        self.maxchunks = maxchunks
        self.iprocessed = 0
        self.suffix = "good" if suffix is None else suffix
        logger.info(f" in flush_chunk {self.chunksize}")

    def flush_chunk(self):
        logger.info(
            f" in flush_chunk: : {len(self.acc)};" f" chunk count : {self.chunk_count}"
        )
        if len(self.acc) > 0:
            filename = f"{self.target_prefix}#{self.suffix}#{self.chunk_count}.json.gz"
            with gzip.GzipFile(filename, "w") as fout:
                fout.write(json.dumps(self.acc, indent=4).encode("utf-8"))
                logger.info(f" flushed {filename}")
                self.chunk_count += 1
                self.iprocessed += len(self.acc)
                self.acc = []

    def push(self, item):
        self.acc.append(item)
        if len(self.acc) >= self.chunksize:
            self.flush_chunk()
            gc.collect()

    def stop(self):
        return self.maxchunks is not None and (self.chunk_count >= self.maxchunks)

    def items_processed(self):
        return self.iprocessed


class FPSmart:
    """
    smart file pointer : acts like a normal file pointer but subs *pattern* with substitute
    """

    def __init__(self, fp, pattern, substitute="", count=0):
        self.fp = fp
        self.pattern = pattern
        self.p = re.compile(self.pattern)
        self.count = count
        self.sub = substitute

    def read(self, n):
        s = self.fp.read(n).decode()
        return self.transform(s).encode()

    def transform(self, s):
        self.p.search(s)
        r = self.p.sub(self.sub, s, count=self.count)
        return r

    def close(self):
        self.fp.close()


tag_wos = "REC"
pattern_wos = r"xmlns=\".*[^\"]\"(?=>)"
force_list_wos = (
    "abstract",
    "address_name",
    "book_note",
    "conf_date",
    "conf_info",
    "conf_location",
    "conf_title",
    "conference",
    "contributor",
    "doctype",
    "grant",
    "grant_id",
    "heading",
    "identifier",
    "keyword",
    "language",
    "name",
    "organization",
    "p",
    "publisher",
    "reference",
    "rw_author",
    "sponsor",
    "subheading",
    "subject",
    "suborganization",
    "title",
    "edition",
    "zip",
)


@contextmanager
def nullcontext(enter_result=None):
    yield enter_result


def gunzip_file(fname_in, fname_out):
    with gzip.open(fname_in, "rb") as f_in:
        with open(fname_out, "wb") as f_out:
            copyfileobj(f_in, f_out)


def parse_simple(fp, good_cf, force_list=None, root_tag=None):
    """
    driver func, parse file fp, push good and bad records
    accordingly to good_cf and bad_cf

    :param fp: filepointer to be parsed
    :param good_cf: chunk flusher of good records
    :param force_list:
    :param root_tag:
    :return:
    """
    events = ("start", "end")
    tree = et.iterparse(fp, events)
    context = iter(tree)
    event, root = next(context)
    for event, pub in context:
        if event == "end" and (pub.tag == root_tag if root_tag is not None else True):
            item = et.tostring(pub, encoding="utf8", method="xml").decode("utf")
            obj = xmltodict.parse(
                item,
                force_cdata=True,
                force_list=force_list,
            )
            good_cf.push(obj)
            root.clear()
            if good_cf.stop():
                break


def convert(
    source: pathlib.Path,
    target_root: str,
    chunk_size: int = 10000,
    max_chunks=None,
    pattern: str | None = None,
    force_list=None,
    root_tag=None,
):
    logger.info(f" chunksize : {chunk_size} | maxchunks {max_chunks} ")

    good_cf = ChunkFlusherMono(target_root, chunk_size, max_chunks)
    bad_cf = ChunkFlusherMono(target_root, chunk_size, max_chunks, suffix="bad")

    if source.suffix == ".gz":
        open_foo: Callable | gzip.GzipFile = gzip.GzipFile
    elif source.suffix == ".xml":
        open_foo = open
    else:
        raise ValueError("Unknown file type")
    # pylint: disable-next=assignment
    fp: gzip.GzipFile | FPSmart | None

    with open_foo(source, "rb") if isinstance(  # type: ignore
        source, pathlib.Path
    ) else nullcontext() as fp:
        if pattern is not None:
            fp = FPSmart(fp, pattern)
        else:
            fp = fp
        parse_simple(fp, good_cf, force_list, root_tag)

        good_cf.flush_chunk()

        logger.info(f" {good_cf.items_processed()} good records")
        bad_cf.flush_chunk()
        logger.info(f"{bad_cf.items_processed()} bad records")
