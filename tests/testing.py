import enum
import os
import tempfile
import threading
import time
from typing import Any


class Counter(object):
    def __init__(self):
        with tempfile.NamedTemporaryFile('w') as f:
            self.filename = f.name

    def increment(self):
        with open(self.filename, 'a') as f:
            f.write('.')

    def value(self):
        with open(self.filename, 'r') as f:
            return len(f.readline())


class TestMode(enum.Enum):
    THREAD = 1
    PROCESS = 2


def assert_mode(mode: TestMode, pid: int, tid: int):
    if mode == TestMode.THREAD:
        assert pid == os.getpid()
        assert tid != threading.get_ident()
    else:
        assert pid != os.getpid()


def echo(x: Any, *, mode: TestMode, pid: int, tid: int):
    assert_mode(mode, pid, tid)
    time.sleep(0.5)
    return x


def inc(counter: Counter, *, mode: TestMode, pid: int, tid: int):
    assert_mode(mode, pid, tid)
    time.sleep(0.5)
    counter.increment()
