"""Provides common functionalities for testing."""

import enum
import os
import tempfile
import threading
import time
from typing import Any


class Counter(object):
    """A simple counter that can be shared by multiple processes."""

    def __init__(self):
        with tempfile.NamedTemporaryFile("w") as f:
            self.filename = f.name

    def inc(self):
        with open(self.filename, "a", encoding="ascii") as f:
            f.write(".")

    def value(self):
        with open(self.filename, "r", encoding="ascii") as f:
            return len(f.readline())


class TestMode(enum.Enum):
    THREAD = enum.auto()
    PROCESS = enum.auto()


def validate_run_mode(mode: TestMode, pid: int, tid: int):
    if mode == TestMode.THREAD:
        assert pid == os.getpid()
        assert tid != threading.get_ident()
    else:
        assert pid != os.getpid()


def echo(x: Any, *, mode: TestMode, pid: int, tid: int):
    validate_run_mode(mode, pid, tid)
    time.sleep(0.5)
    return x


def inc(counter: Counter, *, mode: TestMode, pid: int, tid: int):
    validate_run_mode(mode, pid, tid)
    time.sleep(0.5)
    counter.inc()
