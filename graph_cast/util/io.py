import gzip
import re


class Chunker:
    def __init__(self, fname, batch_size, n_lines_max=None):
        self.acc = []
        self.j = 0
        self.batch_size = batch_size
        self.n_lines_max = n_lines_max
        if fname[-2:] == "gz":
            self.file_obj = gzip.open(fname, "rt")
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