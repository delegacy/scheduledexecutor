import logging
from concurrent import futures
from concurrent.futures import process
from typing import Optional, Callable, Union

from scheduledexecutor import base, thread

logger = logging.getLogger(__name__)


class ScheduledProcessPoolExecutor:
    class _ScheduledFuture(base.ScheduledFuture):
        def __init__(self):
            super().__init__()

            self._future: Union[None, thread._ScheduledFuture, base.ScheduledFuture] = None

        def cancel(self) -> bool:
            if self._future:
                self._future.cancel()

            return super().cancel()


    def __init__(self, max_workers=None, mp_context=None,
                 initializer=None, initargs=()):

        self._thread_executor = thread.ScheduledThreadPoolExecutor(1)
        self._process_executor = process.ProcessPoolExecutor(
            max_workers, mp_context, initializer, initargs)

    def _decorate_task(self, scheduled_future: base.ScheduledFuture, is_periodic: bool, fn: Callable, args, kwargs):
        def wrapper():
            pf = self._process_executor.submit(fn, *args, **kwargs)
            pf.add_done_callback(done_callback)

        def done_callback(pf: futures.Future):
            try:
                result = pf.result()
                if not is_periodic:
                    scheduled_future.set_result(result)
            except futures.CancelledError:
                pass
            except Exception as e:
                scheduled_future.set_exception(e)
                if is_periodic:
                    scheduled_future.tf.set_exception(e)

        return wrapper

    def _tf_done_callback(self, scheduled_future: base.ScheduledFuture):
        def done_callback(tf: futures.Future):
            try:
                tf.result()
            except futures.CancelledError:
                pass
            except Exception as e:
                scheduled_future.set_exception(e)

        return done_callback

    def schedule(self,
                 delay: float,
                 fn: Callable,
                 /, *args, **kwargs) -> base.ScheduledFuture:

        sf = ScheduledProcessPoolExecutor._ScheduledFuture()
        sf._future = self._thread_executor.schedule(
            delay, self._decorate_task(sf, False, fn, args, kwargs))
        sf._future.add_done_callback(self._tf_done_callback(sf))

        return sf

    def schedule_at_fixed_rate(self,
                               initial_delay: float,
                               period: float,
                               fn: Callable,
                               /, *args, **kwargs) -> base.ScheduledFuture:

        sf = ScheduledProcessPoolExecutor._ScheduledFuture()
        sf._future = (
            self._thread_executor.schedule_at_fixed_rate(
                initial_delay, period,
                self._decorate_task(sf, True, fn, args, kwargs)))
        sf._future.add_done_callback(self._tf_done_callback(sf))

        return sf

    def schedule_at_fixed_delay(self,
                                initial_delay: float,
                                delay: float,
                                fn: Callable,
                                /, *args, **kwargs) -> base.ScheduledFuture:

        if delay <= 0.0:
            raise ValueError(f'delay must be > 0, not {delay}')

        def done_callback(f):
            try:
                f.result()
                sf._future = self.schedule(delay, fn, *args, **kwargs)
                sf._future.add_done_callback(done_callback)
            except futures.CancelledError:
                pass
            except Exception as e:
                sf.set_exception(e)

        sf = ScheduledProcessPoolExecutor._ScheduledFuture()
        sf._future = self.schedule(initial_delay, fn, *args, **kwargs)
        sf._future.add_done_callback(done_callback)

        return sf

    def submit(self,
               fn: Callable,
               /, *args, **kwargs) -> futures.Future:
        return self.schedule(0.0, fn, *args, **kwargs)
