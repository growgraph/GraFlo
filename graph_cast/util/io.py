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
from typing import TypeVar

AbsChunkerType = TypeVar("AbsChunkerType", bound="AbsChunker")

logger = logging.getLogger(__name__)


class AbsChunker(abc.ABC):
    def __init__(self):
        self.units_processed = 0

    def pop(self):
        pass

    def pop_header(self):
        pass

    def done(self):
        pass


class Chunker(AbsChunker):
    def __init__(
        self,
        fname=None,
        pkg_spec=None,
        batch_size=10000,
        n_lines_max: int | None = None,
        encoding="utf-8",
    ):
        """
        WARNING : if data sources are gzipped - batch_size does not correspond to lines, instead it's a proxy for bytes

        :param fname:
        :param batch_size: batch size in bytes : batch_size = 15000 corresponds to 100 lines ~ 100 symbols each
                        for gzipped sources
        :param n_lines_max:
        :param encoding:
        """
        super().__init__()
        if fname is None and pkg_spec is None:
            raise ValueError(f" both fname and file_obj are None")

        self.batch_size = batch_size
        self.n_lines_max: int | None = n_lines_max

        logger.info(
            f"Chunker init with batch_size : {self.batch_size} n_lines_max {self.n_lines_max}"
        )
        if fname is not None:
            if fname[-2:] == "gz":
                self.file_obj = gzip.open(fname, "rt", encoding=encoding)
            else:
                self.file_obj = open(fname, "rt")
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

    @property
    def done(self):
        return self._done


class ChunkerDataFrame(AbsChunker):
    def __init__(self, df, batch_size, n_lines_max=None):
        super().__init__()
        self.batch_size = batch_size
        self.n_lines_max = n_lines_max
        self.file_obj = df
        self.done = False
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
                self.done = True
                return False
            else:
                return lines
        else:
            self.done = True
            return False

    def done(self):
        return self.done


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
            f" in flush_chunk: len(self.acc) : {len(self.acc)}; self.chunk_count : {self.chunk_count}"
        )
        if len(self.acc) > 0:
            fname = f"{self.target_prefix}#{self.suffix}#{self.chunk_count}.json.gz"
            with gzip.GzipFile(fname, "w") as fout:
                fout.write(json.dumps(self.acc, indent=4).encode("utf-8"))
                logger.info(f" flushed {fname}")
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
