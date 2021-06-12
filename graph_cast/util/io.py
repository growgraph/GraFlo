import gzip
import re
import json
import logging

logger = logging.getLogger(__name__)


class Chunker:
    def __init__(self, fname, batch_size, n_lines_max=None, encoding="utf-8"):
        self.acc = []
        self.j = 0
        self.batch_size = batch_size
        self.n_lines_max = n_lines_max
        if fname[-2:] == "gz":
            self.file_obj = gzip.open(fname, "rt", encoding=encoding)
        else:
            self.file_obj = open(fname, "rt")
        self.done = False

    def pop_header(self):
        return self.file_obj.readline().rstrip("\n")

    def pop(self):
        if not self.n_lines_max or (self.n_lines_max and self.j < self.n_lines_max):
            lines = self.file_obj.readlines(self.batch_size)
            self.j += len(lines)
            if not lines:
                self.done = True
                self.file_obj.close()
                return False
            else:
                return lines
        else:
            self.done = True
            self.file_obj.close()
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
        m = self.p.search(s)
        r = self.p.sub(self.sub, s, count=self.count)
        return r

    def close(self):
        self.fp.close()
