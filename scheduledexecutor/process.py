"""Provides :class:`ProcessPoolExecutor`."""

from concurrent import futures
from concurrent.futures import process
from typing import Any, Callable, Dict, Tuple, Union

from scheduledexecutor import base, thread


class _ScheduledFuture(base.ScheduledFuture):
    """:class:`ProcessPoolExecutor`-specific :class:`scheduledexecutor.ScheduledFuture`."""

    def __init__(self):
        super().__init__()

        self.future: Union[None, thread._ScheduledFuture, base.ScheduledFuture] = None

    def cancel(self) -> bool:
        if self.future:
            self.future.cancel()

        return super().cancel()


def _tf_done_callback(future: base.ScheduledFuture):
    def wrapper(tf: futures.Future):
        try:
            tf.result()
        except futures.CancelledError:
            pass
        except Exception as e:  # pylint:disable=broad-except
            future.set_exception(e)

    return wrapper


class ProcessPoolExecutor:
    """Extends :class:`concurrent.futures.ProcessPoolExecutor` to enable delayed and/or recurring tasks."""

    def __init__(
        self, max_workers=None, mp_context=None, initializer=None, initargs=()
    ):

        self._thread_executor: thread.ThreadPoolExecutor = thread.ThreadPoolExecutor(1)
        self._process_executor: process.ProcessPoolExecutor = (
            process.ProcessPoolExecutor(max_workers, mp_context, initializer, initargs)
        )

    def _make_task(
        self,
        future: base.ScheduledFuture,
        is_periodic: bool,
        fn: Callable,
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
    ):
        def pf_done_callback(pf: futures.Future):
            try:
                result = pf.result()
                if not is_periodic:
                    future.set_result(result)
            except futures.CancelledError:
                pass
            except Exception as e:  # pylint:disable=broad-except
                future.set_exception(e)
                if is_periodic and hasattr(future, "future"):
                    future.future.set_exception(e)

        def wrapper():
            pf = self._process_executor.submit(fn, *args, **kwargs)
            pf.add_done_callback(pf_done_callback)

        return wrapper

    def schedule(
        self, delay: float, fn: Callable, *args, **kwargs
    ) -> base.ScheduledFuture:

        sf = _ScheduledFuture()
        sf.future = self._thread_executor.schedule(
            delay, self._make_task(sf, False, fn, args, kwargs)
        )
        sf.future.add_done_callback(_tf_done_callback(sf))

        return sf

    def schedule_at_fixed_rate(
        self, initial_delay: float, period: float, fn: Callable, *args, **kwargs
    ) -> base.ScheduledFuture:

        sf = _ScheduledFuture()
        sf.future = self._thread_executor.schedule_at_fixed_rate(
            initial_delay, period, self._make_task(sf, True, fn, args, kwargs)
        )
        sf.future.add_done_callback(_tf_done_callback(sf))

        return sf

    def schedule_at_fixed_delay(
        self, initial_delay: float, delay: float, fn: Callable, *args, **kwargs
    ) -> base.ScheduledFuture:

        if delay <= 0.0:
            raise ValueError(f"delay must be > 0, not {delay}")

        def done_callback(f):
            try:
                f.result()
                sf.future = self.schedule(delay, fn, *args, **kwargs)
                sf.future.add_done_callback(done_callback)
            except futures.CancelledError:
                pass
            except Exception as e:  # pylint:disable=broad-except
                sf.set_exception(e)

        sf = _ScheduledFuture()
        sf.future = self.schedule(initial_delay, fn, *args, **kwargs)
        sf.future.add_done_callback(done_callback)

        return sf

    def submit(self, fn: Callable, *args, **kwargs) -> futures.Future:
        return self.schedule(0.0, fn, *args, **kwargs)

    @property
    def pool_size(self):
        # pylint:disable=protected-access
        return len(self._process_executor._processes)

    @property
    def max_pool_size(self):
        # pylint:disable=protected-access
        return self._process_executor._max_workers

    @property
    def queued_task_count(self):
        return self._thread_executor.queued_task_count + len(
            self._process_executor._pending_work_items  # pylint:disable=protected-access
        )
