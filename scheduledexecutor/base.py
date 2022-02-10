from concurrent import futures
import heapq
import queue
import threading
import time


class ScheduledFuture(futures.Future):
    pass


class DelayQueue(queue.Queue):
    def __init__(self, maxsize=0):
        super().__init__(maxsize)

        # noinspection PyTypeChecker
        self.mutex = threading.RLock()
        self.not_empty = threading.Condition(self.mutex)
        self.not_full = threading.Condition(self.mutex)
        self.all_tasks_done = threading.Condition(self.mutex)

        self.queue = []

    def _put(self, item):
        item_time = item.time if item else 0.0
        heapq.heappush(self.queue, (item_time, item))

    def _get(self):
        return heapq.heappop(self.queue)[1]

    def get(self, block=True, timeout=None):
        deadline = time.time() + timeout if timeout is not None else None
        with self.not_empty:
            while True:
                if not self._qsize():
                    if not block:
                        raise queue.Empty
                    elif deadline is None:
                        self.not_empty.wait()
                    else:
                        remaining = deadline - time.time()
                        if remaining <= 0.0:
                            raise queue.Empty
                        self.not_empty.wait(remaining)
                    continue

                head = self.queue[0][1]
                delay = head.delay() if head else 0.0
                if delay > 0.0:
                    if not block:
                        raise queue.Empty
                    elif deadline is None:
                        self.not_empty.wait(delay)
                    else:
                        remaining = deadline - time.time()
                        if remaining <= 0.0:
                            raise queue.Empty
                        self.not_empty.wait(min(delay, remaining))
                else:
                    item = self._get()
                    self.not_full.notify()
                    return item
