"""Tests ProcessPoolExecutor."""

import os
import random
import threading
import time

import pytest

import scheduledexecutor as executors
from tests import testing


def test_submit():
    x = random.random()
    executor = executors.ProcessPoolExecutor(2)
    assert executor.pool_size == 0
    assert executor.max_pool_size == 2
    assert executor.queued_task_count == 0
    f = executor.submit(
        testing.echo,
        x,
        mode=testing.TestMode.PROCESS,
        pid=os.getpid(),
        tid=threading.get_ident(),
    )
    assert f.result() == x
    assert (
        executor.pool_size >= 1
    )  # multiple worker processes can be spawned for Python 3.8 and below.
    assert executor.queued_task_count == 0


def test_schedule():
    x = random.random()
    executor = executors.ProcessPoolExecutor()
    t = time.time()
    f = executor.schedule(
        1.0,
        testing.echo,
        x,
        mode=testing.TestMode.PROCESS,
        pid=os.getpid(),
        tid=threading.get_ident(),
    )
    assert executor.pool_size == 0
    assert executor.queued_task_count == 1
    assert f.result() == x
    assert time.time() >= t + 1.0
    assert executor.pool_size >= 1
    assert executor.queued_task_count == 0


def test_schedule_should_raise_when_negative_delay():
    executor = executors.ProcessPoolExecutor()
    with pytest.raises(ValueError):
        executor.schedule(
            -1.0,
            testing.echo,
            random.random(),
            mode=testing.TestMode.PROCESS,
            pid=os.getpid(),
            tid=threading.get_ident(),
        )


def test_schedule_at_fixed_rate():
    counter = testing.Counter()
    executor = executors.ProcessPoolExecutor()
    f = executor.schedule_at_fixed_rate(
        1.0,
        1.0,
        testing.inc,
        counter,
        mode=testing.TestMode.PROCESS,
        pid=os.getpid(),
        tid=threading.get_ident(),
    )
    time.sleep(5.2)
    assert f.cancel() is True
    assert counter.value() == 4


def test_schedule_at_fixed_rate_should_raise_when_negative_period():
    executor = executors.ProcessPoolExecutor()
    with pytest.raises(ValueError):
        executor.schedule_at_fixed_rate(
            1.0,
            -1.0,
            testing.echo,
            random.random(),
            mode=testing.TestMode.PROCESS,
            pid=os.getpid(),
            tid=threading.get_ident(),
        )


def test_schedule_at_fixed_delay():
    counter = testing.Counter()
    executor = executors.ProcessPoolExecutor()
    f = executor.schedule_at_fixed_delay(
        1.0,
        1.0,
        testing.inc,
        counter,
        mode=testing.TestMode.PROCESS,
        pid=os.getpid(),
        tid=threading.get_ident(),
    )
    time.sleep(5.2)
    assert f.cancel() is True
    assert counter.value() == 3


def test_schedule_at_fixed_delay_should_raise_when_negative_delay():
    executor = executors.ProcessPoolExecutor()
    with pytest.raises(ValueError):
        executor.schedule_at_fixed_delay(
            1.0,
            -1.0,
            testing.echo,
            random.random(),
            mode=testing.TestMode.PROCESS,
            pid=os.getpid(),
            tid=threading.get_ident(),
        )
