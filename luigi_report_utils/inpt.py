"""
A wrapper for file and string objects that provides an interface compatible with luigi task inputs
"""

import io
import logging

logger = logging.getLogger("luigi_report_utils.inpt")


class BaseInpt:

    # See: https://docs.python.org/3/reference/datamodel.html#object.__enter__
    def __enter__(self):
        self._handle = self.open()
        return self._handle

    # See: https://docs.python.org/3/reference/datamodel.html#object.__exit__
    def __exit__(self, exc_type, exc_value, traceback):
        self._handle.close()

    def open(self):
        raise Exception("this method must be overriden")


class FilePathInpt(BaseInpt):
    def __init__(self, path):
        self.path = path

    def open(self, mode="r"):
        return open(self.path, mode)

    def __str__(self):
        return "<FilePathInpt({})>".format(self.path.__repr__())


class BufferInpt(BaseInpt):
    def __init__(self, buffer):
        self.buffer = buffer

    def open(self, mode="r"):
        if mode == "r":
            return io.StringIO(self.buffer.decode("utf-8"))
        if mode == "rb":
            return io.BytesIO(self.buffer)

    def __str__(self):
        return "<BufferInpt(...)>"


def from_path(path):
    return FilePathInpt(path)


def from_str(data, encoding="utf-8"):
    return from_bytes(data.encode(encoding))


def from_bytes(buffer):
    return BufferInpt(buffer)
