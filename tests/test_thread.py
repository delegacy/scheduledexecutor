"""Tests ThreadPoolExecutor."""

import functools
import os
import random
import threading
import time

import pytest

import scheduledexecutor as executors
from tests import testing


def test_submit():
    x = random.random()
    executor = executors.ThreadPoolExecutor(2)
    assert executor.pool_size == 0
    assert executor.max_pool_size == 2
    assert executor.queued_task_count == 0
    f = executor.submit(
        testing.echo,
        x,
        mode=testing.TestMode.THREAD,
        pid=os.getpid(),
        tid=threading.get_ident(),
    )
    assert f.result() == x
    assert executor.pool_size == 1
    assert executor.queued_task_count == 0


def test_submit_with_task_decorator():
    def make_task_decorator(offset):
        def task_decorator(fn):
            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                return fn(*args, **kwargs) + offset

            return wrapper

        return task_decorator

    x = random.random()
    y = random.random()
    executor = executors.ThreadPoolExecutor()
    executor.task_decorator = make_task_decorator(y)
    f = executor.submit(
        testing.echo,
        x,
        mode=testing.TestMode.THREAD,
        pid=os.getpid(),
        tid=threading.get_ident(),
    )
    assert f.result() == x + y


def test_schedule():
    x = random.random()
    executor = executors.ThreadPoolExecutor()
    t = time.time()
    f = executor.schedule(
        1.0,
        testing.echo,
        x,
        mode=testing.TestMode.THREAD,
        pid=os.getpid(),
        tid=threading.get_ident(),
    )
    assert executor.pool_size == 1
    assert executor.queued_task_count == 1
    assert f.result() == x
    assert time.time() >= t + 1.0
    assert executor.pool_size == 1
    assert executor.queued_task_count == 0


def test_schedule_should_raise_when_negative_delay():
    executor = executors.ThreadPoolExecutor()
    with pytest.raises(ValueError):
        executor.schedule(
            -1.0,
            testing.echo,
            random.random(),
            mode=testing.TestMode.THREAD,
            pid=os.getpid(),
            tid=threading.get_ident(),
        )


def test_schedule_at_fixed_rate():
    counter = testing.Counter()
    executor = executors.ThreadPoolExecutor()
    f = executor.schedule_at_fixed_rate(
        1.0,
        1.0,
        testing.inc,
        counter,
        mode=testing.TestMode.THREAD,
        pid=os.getpid(),
        tid=threading.get_ident(),
    )
    time.sleep(5.2)
    f.cancel()
    assert counter.value() == 4


def test_schedule_at_fixed_rate_should_raise_when_negative_period():
    executor = executors.ThreadPoolExecutor()
    with pytest.raises(ValueError):
        executor.schedule_at_fixed_rate(
            1.0,
            -1.0,
            testing.echo,
            random.random(),
            mode=testing.TestMode.THREAD,
            pid=os.getpid(),
            tid=threading.get_ident(),
        )


def test_schedule_at_fixed_delay():
    counter = testing.Counter()
    executor = executors.ThreadPoolExecutor()
    f = executor.schedule_at_fixed_delay(
        1.0,
        1.0,
        testing.inc,
        counter,
        mode=testing.TestMode.THREAD,
        pid=os.getpid(),
        tid=threading.get_ident(),
    )
    time.sleep(5.2)
    f.cancel()
    assert counter.value() == 3


def test_schedule_at_fixed_delay_should_raise_when_negative_delay():
    executor = executors.ThreadPoolExecutor()
    with pytest.raises(ValueError):
        executor.schedule_at_fixed_delay(
            1.0,
            -1.0,
            testing.echo,
            random.random(),
            mode=testing.TestMode.THREAD,
            pid=os.getpid(),
            tid=threading.get_ident(),
        )
