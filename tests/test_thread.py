"""Tests ScheduledThreadPoolExecutor."""

import os
import random
import threading
import time

import pytest

import scheduledexecutor as executors
from tests import testing


def test_submit():
    x = random.random()
    executor = executors.ThreadPoolExecutor()
    f = executor.submit(
        testing.echo,
        x,
        mode=testing.TestMode.THREAD,
        pid=os.getpid(),
        tid=threading.get_ident(),
    )
    assert f.result() == x


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
    assert f.result() == x
    assert time.time() >= t + 1.0


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
