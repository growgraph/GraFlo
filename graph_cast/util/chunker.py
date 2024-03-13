from __future__ import annotations

import abc
import gc
import gzip
import json
import logging
import re
from pathlib import Path
from typing import Any, TextIO, TypeVar

import ijson
import pandas as pd

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
        super().__init__(**kwargs)
        kwargs_ = dict(kwargs)
        kwargs_["chunksize"] = kwargs_.pop("batch_size")
        kwargs_["nrows"] = kwargs_.pop("limit")
        kwargs_.pop("filename")
        self.file_obj = pd.read_csv(self.filename, **kwargs_)

    def _prepare_iteration(self):
        self.header = pd.read_csv(
            self.filename, index_col=None, nrows=0
        ).columns.tolist()

    def __next__(self):
        item = next(self.file_obj)
        dressed = item.to_dict("records")
        self.cnt += len(dressed)
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
                # if ".tsv" in resource.suffixes:

                # return pd.read_csv(resource, **kwargs_)
                # chunksize=chunksize)
                return TableChunker(filename=resource, **kwargs)
                # else:
                #     return TableChunker(filename=resource, sep=",", **kwargs)
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
            f" in flush_chunk: len(self.acc) : {len(self.acc)};"
            f" self.chunk_count : {self.chunk_count}"
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
