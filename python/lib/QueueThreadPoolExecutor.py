import concurrent
import queue
import threading
import time
from concurrent.futures import Future
from typing import Callable, List, Dict, Tuple


class BoundedThreadRunner:
    def __init__(self, pool_maxsize, do_task: Callable):
        self.pool_maxsize = pool_maxsize
        self.task_queue = queue.Queue(maxsize=pool_maxsize)
        self.do_task = do_task
        self.workers = []
        self.is_stopped = False

    def start(self) -> List[threading.Thread]:
        for i in range(self.pool_maxsize):
            thread = threading.Thread(target=self._run)
            thread.start()
            self.workers.append(thread)

        return self.workers

    def submit(self, value):
        self.task_queue.put(value)

    def _run(self):
        while True:
            value = self.task_queue.get()  # Block until a task is available
            if value is None and self.is_stopped:
                self.task_queue.task_done()
                break

            self.do_task(value)
            self.task_queue.task_done()

    def stop(self):
        self.task_queue.join()

        self.is_stopped = True
        for _ in range(self.pool_maxsize):
            self.submit(None)

        for thread in self.workers:
            thread.join()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()


class BoundedThreadPoolExecutor(concurrent.futures.ThreadPoolExecutor):
    def __init__(self, do_task: Callable, done_callback_fn: Callable, max_workers: int, thread_name_prefix=''):
        self.semaphore = threading.BoundedSemaphore(max_workers)

        super().__init__(max_workers, thread_name_prefix)
        # self._work_queue = queue.Queue(maxsize=max_workers)

        self.do_task = do_task
        self.done_callback = done_callback_fn
        self.lock = threading.Lock()
        self.futures: Dict[Future, Tuple[float, Tuple]] = {}

    def submit(self, *args, **kwargs):
        self.semaphore.acquire()
        try:
            # Forward *args/**kwargs to the wrapped task to preserve full signature
            future = super().submit(self.do_task, *args, **kwargs)
            with self.lock:
                self.futures[future] = (time.time(), args)

            future.add_done_callback(self._done_callback)
            return future
        except Exception:
            self.semaphore.release()  # ← prevent permit leak
            raise

    def _done_callback(self, future):
        # Release permit first so queue progress isn't blocked by callback work
        self.semaphore.release()
        try:
            self.done_callback(future)
        except Exception as cb_err:
            # Swallow callback exceptions to avoid disrupting the executor's internal machinery
            print(f"Callback error: {cb_err}")
        finally:
            with self.lock:
                self.futures.pop(future, None)

    def futures_timeout_args(self) -> Dict[Future, Tuple[float, Tuple]]:
        with self.lock:
            return dict(self.futures)

    def undone_futures_args(self, timeout: float = None) -> List[Tuple[float, Tuple]]:
        with self.lock:
            if timeout is None:
                return [v for k, v in self.futures.items() if not k.done()]
            else:
                return [v for k, v in self.futures.items() if not k.done() and time.time() - v[0] > timeout]
