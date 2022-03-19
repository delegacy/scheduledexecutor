import logging
import sys
import time
from concurrent import futures
from concurrent.futures import thread, _base as futures_base
from typing import Callable

from scheduledexecutor import base

logger = logging.getLogger(__name__)


def _trigger_time(delay):
    return time.time() + delay


class _ScheduledWorkItem(thread._WorkItem):
    def __init__(self, future, fn, args, kwargs, *, executor, time, period):
        super().__init__(future, fn, args, kwargs)

        self.executor = executor
        self.time = time
        self.period = period

    def delay(self) -> float:
        return self.time - time.time()

    def is_periodic(self) -> bool:
        return self.period != 0.0

    def set_next_run_time(self) -> None:
        p = self.period
        if p > 0:
            self.time += p
        else:
            self.time = _trigger_time(-p)

    def run(self) -> None:
        if not self.is_periodic():
            super().run()
            return

        if self.future.notify_cancel_if_cancelled():
            return

        try:
            self.fn(*self.args, **self.kwargs)
        except BaseException as e:
            self.future.set_exception(e)
            self = None
        else:
            self.set_next_run_time()

            self.executor._re_execute_periodic(self)


class ScheduledThreadPoolExecutor(futures.ThreadPoolExecutor):
    class _ScheduledFuture(base.ScheduledFuture):
        def __init__(self):
            super().__init__()

        def notify_cancel_if_cancelled(self):
            with self._condition:
                if self._state != futures_base.CANCELLED:
                    return False

                self._state = futures_base.CANCELLED_AND_NOTIFIED
                for waiter in self._waiters:
                    waiter.add_cancelled(self)
                return True

    def __init__(
        self, max_workers=None, thread_name_prefix="", initializer=None, initargs=()
    ):
        super().__init__(max_workers, thread_name_prefix, initializer, initargs)

        self._work_queue = base.DelayQueue()

    def _delayed_execute(self, work_item: _ScheduledWorkItem):
        self._work_queue.put(work_item)
        self._adjust_thread_count()

    def _re_execute_periodic(self, work_item: _ScheduledWorkItem):
        self._work_queue.put(work_item)
        self._adjust_thread_count()

    def _schedule(self, initial_delay, period, fn, *args, **kwargs):
        if initial_delay < 0.0:
            raise ValueError(f"initial_delay must be >= 0, not {initial_delay}")

        if sys.version_info >= (3, 9):
            with self._shutdown_lock, thread._global_shutdown_lock:
                return self._schedule_within_lock(
                    initial_delay, period, fn, *args, **kwargs
                )
        else:
            with self._shutdown_lock:
                return self._schedule_within_lock(
                    initial_delay, period, fn, *args, **kwargs
                )

    def _schedule_within_lock(self, initial_delay, period, fn, *args, **kwargs):
        if self._broken:
            raise thread.BrokenThreadPool(self._broken)

        if self._shutdown:
            raise RuntimeError("cannot schedule new futures after shutdown")
        if thread._shutdown:
            raise RuntimeError(
                "cannot schedule new futures after " "interpreter shutdown"
            )

        f = ScheduledThreadPoolExecutor._ScheduledFuture()
        w = _ScheduledWorkItem(
            f,
            fn,
            args,
            kwargs,
            executor=self,
            time=_trigger_time(initial_delay),
            period=period,
        )

        self._delayed_execute(w)
        return f

    def schedule(self, delay: float, fn: Callable, *args, **kwargs):
        return self._schedule(delay, 0.0, fn, *args, **kwargs)

    def schedule_at_fixed_rate(
        self, initial_delay: float, period: float, fn: Callable, *args, **kwargs
    ):
        if period <= 0.0:
            raise ValueError(f"period must be > 0, not {period}")

        return self._schedule(initial_delay, period, fn, *args, **kwargs)

    def schedule_at_fixed_delay(
        self, initial_delay: float, delay: float, fn: Callable, *args, **kwargs
    ):
        if delay <= 0.0:
            raise ValueError(f"delay must be > 0, not {delay}")

        return self._schedule(initial_delay, -delay, fn, *args, **kwargs)

    def submit(self, fn: Callable, *args, **kwargs):
        return self.schedule(0.0, fn, *args, **kwargs)
