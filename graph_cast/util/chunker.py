from __future__ import annotations

import abc
import csv
import gc
import gzip
import io
import json
import logging
import pkgutil
import re
from pathlib import Path
from typing import Any, TextIO, TypeVar

import ijson

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

    @abc.abstractmethod
    def __next__(self):
        pass

    @abc.abstractmethod
    def _next_item(self):
        pass

    def _prepare_iteration(self):
        self.iteration_tried = True


class FileChunkerNew(AbstractChunker):
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
        self.encoding: EncodingType = encoding
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


class TableChunker(FileChunkerNew):
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
            next(csv.reader([line.rstrip()], skipinitialspace=True))
            for line in lines
        ]
        dressed = [dict(zip(self.header, row)) for row in lines2]
        return dressed


class JsonlChunker(FileChunkerNew):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __next__(self):
        lines = super().__next__()
        lines2 = [json.loads(line) for line in lines]
        return lines2


class JsonChunker(FileChunkerNew):
    def __init__(self, **kwargs):
        super().__init__(mode="b", **kwargs)
        self.parser: Any

    def _prepare_iteration(self):
        super()._prepare_iteration()
        self.parser = ijson.items(self.file_obj, "item")

    def _next_item(self):
        return next(self.parser)


class TrivialChunker(AbstractChunker):
    def __init__(self, array, **kwargs):
        super().__init__(**kwargs)
        self.array = array

    def _prepare_iteration(self):
        pass

    def _next_item(self):
        pass

    def __next__(self):
        cid = self.cnt
        batch = self.array[cid : cid + self.batch_size]
        self.cnt += len(batch)
        if not batch or self._limit_reached():
            raise StopIteration
        return batch


class ChunkerFactory:
    @classmethod
    def create_chunker(cls, **kwargs) -> AbstractChunker:
        filename: Path | None = kwargs.get("filename", None)
        chunker_type = kwargs.pop("type", None)
        if filename is None:
            return TrivialChunker(**kwargs)
        elif chunker_type == ChunkerType.JSONL or (
            chunker_type is None and ".jsonl" in filename.suffixes
        ):
            return JsonlChunker(**kwargs)
        elif chunker_type == ChunkerType.JSON or (
            chunker_type is None and ".json" in filename.suffixes
        ):
            return JsonChunker(**kwargs)
        elif chunker_type == ChunkerType.TABLE or (
            chunker_type is None
            and any([".csv" in filename.suffixes, ".tsv" in filename.suffixes])
        ):
            if ".tsv" in filename.suffixes:
                return TableChunker(sep="\t", **kwargs)
            else:
                return TableChunker(sep=",", **kwargs)
        raise ValueError(
            "Could not determine the type of required Chunker "
            f"for type={chunker_type} and filename={filename}"
        )


class FileChunker(AbstractChunker):
    def __init__(
        self,
        filename=None,
        pkg_spec=None,
        batch_size=10000,
        limit: int | None = None,
        encoding=EncodingType.UTF_8,
    ):
        """
        WARNING : if misc sources are gzipped - batch_size does not correspond to lines, instead it's a proxy for bytes

        :param filename:
        :param batch_size: batch size in bytes : batch_size = 15000 corresponds to 100 lines ~ 100 symbols each
                        for gzipped sources
        :param limit:
        :param encoding:
        """
        super().__init__()
        if filename is None and pkg_spec is None:
            raise ValueError(f" both filename and file_obj are None")

        self.batch_size = batch_size
        self.n_lines_max: int | None = limit

        logger.info(
            f"Chunker init with batch_size : {self.batch_size} n_lines_max"
            f" {self.n_lines_max}"
        )
        if filename is not None:
            if filename[-2:] == "gz":
                self.file_obj = gzip.open(filename, "rt", encoding=encoding)
            else:
                self.file_obj = open(filename, "rt")
        else:
            bytes_ = pkgutil.get_data(*pkg_spec)
            if isinstance(bytes_, bytes):
                if pkg_spec[1][-2:] == "gz":
                    self.file_obj = gzip.GzipFile(
                        fileobj=io.BytesIO(bytes_), mode="r"
                    )
                else:
                    self.file_obj = io.BytesIO(bytes_)
            else:
                raise TypeError(f"bytes_ should be a bytes Type")

            self.file_obj = io.TextIOWrapper(self.file_obj, encoding="utf-8")  # type: ignore
        self._done = False

    def pop_header(self):
        header = self.file_obj.readline().rstrip("\n")
        header = header.split(",")
        return header

    def pop(self):
        if self.n_lines_max is None or (
            self.n_lines_max is not None
            and self.units_processed < self.n_lines_max
        ):
            lines = self.file_obj.readlines(self.batch_size)
            lines2 = [
                next(csv.reader([line.rstrip()], skipinitialspace=True))
                for line in lines
            ]
            if self.n_lines_max is not None and (
                self.units_processed + len(lines2) > self.n_lines_max
            ):
                lines2 = lines2[: (self.n_lines_max - self.units_processed)]
            self.units_processed += len(lines2)
            if not lines2:
                self._done = True
                self.file_obj.close()
                return []
            else:
                return lines2
        else:
            self._done = True
            self.file_obj.close()
            return []


class ChunkerDataFrame(AbstractChunker):
    def __init__(self, df, batch_size, n_lines_max=None):
        super().__init__()
        self.batch_size = batch_size
        self.n_lines_max = n_lines_max
        self.file_obj = df
        self._done = False
        self.idx = [
            i for i in range(0, self.file_obj.shape[0], self.batch_size)
        ][::-1]

    def pop_header(self):
        return self.file_obj.columns

    def pop(self):
        if self.idx:
            cid = self.idx.pop()
            lines = self.file_obj.iloc[
                cid : cid + self.batch_size
            ].values.tolist()
            self.units_processed += len(lines)
            if not lines:
                self._done = True
                return False
            else:
                return lines
        else:
            self._done = True
            return False


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
        return self.maxchunks is not None and (
            self.chunk_count >= self.maxchunks
        )

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
