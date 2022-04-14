"""Provides :class:`ThreadPoolExecutor`."""

from __future__ import annotations

import contextlib
import queue
import sys
import time
from concurrent import futures
from concurrent.futures import _base, thread
from typing import Any, Callable, Dict, Optional, Tuple

from scheduledexecutor import base


def _trigger_time(delay: float) -> float:
    return time.time() + delay


class _ScheduledFuture(base.ScheduledFuture):
    """:class:`ThreadPoolExecutor`-specific :class:`scheduledexecutor.base.ScheduledFuture`."""

    def notify_cancel_if_cancelled(self) -> bool:
        with self._condition:
            # defined in super. pylint: disable=access-member-before-definition
            if self._state != _base.CANCELLED:
                return False

            self._state = _base.CANCELLED_AND_NOTIFIED
            for waiter in self._waiters:
                waiter.add_cancelled(self)
            return True


class _ScheduledWorkItem(thread._WorkItem):  # pylint: disable=protected-access
    """:class:`ThreadPoolExecutor`-specific :class:`concurrent.futures.thread._WorkItem`."""

    def __init__(
        self,
        future: _ScheduledFuture,
        fn: Callable,
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        *,
        executor: ThreadPoolExecutor,
        trigger_time: float,
        period: float,
    ):
        super().__init__(future, fn, args, kwargs)

        self.executor: ThreadPoolExecutor = executor
        self.trigger_time: float = trigger_time
        self.period: float = period

    def delay(self) -> float:
        return self.trigger_time - time.time()

    def is_periodic(self) -> bool:
        return self.period != 0.0

    def set_next_run_time(self) -> None:
        p = self.period
        if p > 0:
            self.trigger_time += p
        else:
            self.trigger_time = _trigger_time(-p)

    def run(self) -> None:
        if not self.is_periodic():
            super().run()
            return

        if self.future.notify_cancel_if_cancelled():
            return

        try:
            self.fn(*self.args, **self.kwargs)
        except Exception as e:  # pylint:disable=broad-except
            self.future.set_exception(e)
            self = None  # pylint:disable=self-cls-assignment
        else:
            self.set_next_run_time()

            # pylint: disable=protected-access
            self.executor._re_execute_periodic(self)


class ThreadPoolExecutor(futures.ThreadPoolExecutor):
    """Extends :class:`concurrent.futures.ThreadPoolExecutor` to enable delayed and/or recurring tasks."""

    def __init__(
        self,
        max_workers: Optional[int] = None,
        thread_name_prefix: str = "",
        initializer: Callable = None,
        initargs: Tuple[Any, ...] = (),
    ):
        super().__init__(max_workers, thread_name_prefix, initializer, initargs)

        self._work_queue: queue.Queue = base.DelayQueue()

        self.task_decorator: Optional[Callable[[Callable], Callable]] = None

    def _delayed_execute(self, work_item: _ScheduledWorkItem) -> None:
        self._work_queue.put(work_item)
        self._adjust_thread_count()

    def _re_execute_periodic(self, work_item: _ScheduledWorkItem) -> None:
        self._work_queue.put(work_item)
        self._adjust_thread_count()

    def _schedule(
        self, initial_delay: float, period: float, fn: Callable, *args, **kwargs
    ) -> _ScheduledFuture:
        if initial_delay < 0.0:
            raise ValueError(f"initial_delay must be >= 0, not {initial_delay}")

        if sys.version_info >= (3, 9):
            # pylint: disable=protected-access
            ctx_managers = [self._shutdown_lock, thread._global_shutdown_lock]
        else:
            ctx_managers = [self._shutdown_lock]

        with contextlib.ExitStack() as stack:
            for ctx_manager in ctx_managers:
                stack.enter_context(ctx_manager)

            if self._broken:
                raise thread.BrokenThreadPool(self._broken)

            if self._shutdown:
                raise RuntimeError("cannot schedule new futures after shutdown")
            if thread._shutdown:  # pylint: disable=protected-access
                raise RuntimeError(
                    "cannot schedule new futures after interpreter shutdown"
                )

            f = _ScheduledFuture()
            w = _ScheduledWorkItem(
                f,
                fn
                if self.task_decorator is None
                else self.task_decorator(fn),  # pylint:disable=not-callable
                args,
                kwargs,
                executor=self,
                trigger_time=_trigger_time(initial_delay),
                period=period,
            )

            self._delayed_execute(w)
            return f

    def schedule(
        self, delay: float, fn: Callable, *args, **kwargs
    ) -> base.ScheduledFuture:
        return self._schedule(delay, 0.0, fn, *args, **kwargs)

    def schedule_at_fixed_rate(
        self, initial_delay: float, period: float, fn: Callable, *args, **kwargs
    ) -> base.ScheduledFuture:
        if period <= 0.0:
            raise ValueError(f"period must be > 0, not {period}")

        return self._schedule(initial_delay, period, fn, *args, **kwargs)

    def schedule_at_fixed_delay(
        self, initial_delay: float, delay: float, fn: Callable, *args, **kwargs
    ) -> base.ScheduledFuture:
        if delay <= 0.0:
            raise ValueError(f"delay must be > 0, not {delay}")

        return self._schedule(initial_delay, -delay, fn, *args, **kwargs)

    def submit(self, fn: Callable, *args, **kwargs) -> base.ScheduledFuture:
        return self.schedule(0.0, fn, *args, **kwargs)

    @property
    def pool_size(self):
        return len(self._threads)

    @property
    def max_pool_size(self):
        return self._max_workers

    @property
    def queued_task_count(self):
        # pylint:disable=protected-access
        return self._work_queue._qsize()
