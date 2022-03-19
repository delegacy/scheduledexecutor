import os
import random
import threading
import time

import pytest

import scheduledexecutor
from tests import testing


def test_submit():
    x = random.random()
    executor = scheduledexecutor.ScheduledProcessPoolExecutor()
    f = executor.submit(
        testing.echo,
        x,
        mode=testing.TestMode.PROCESS,
        pid=os.getpid(),
        tid=threading.get_ident(),
    )
    assert f.result() == x


def test_schedule():
    x = random.random()
    executor = scheduledexecutor.ScheduledProcessPoolExecutor()
    t = time.time()
    f = executor.schedule(
        1.0,
        testing.echo,
        x,
        mode=testing.TestMode.PROCESS,
        pid=os.getpid(),
        tid=threading.get_ident(),
    )
    assert f.result() == x
    assert time.time() >= t + 1.0


def test_schedule_should_raise_when_negative_delay():
    executor = scheduledexecutor.ScheduledProcessPoolExecutor()
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
    executor = scheduledexecutor.ScheduledProcessPoolExecutor()
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
    executor = scheduledexecutor.ScheduledProcessPoolExecutor()
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
    executor = scheduledexecutor.ScheduledProcessPoolExecutor()
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
    executor = scheduledexecutor.ScheduledProcessPoolExecutor()
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
