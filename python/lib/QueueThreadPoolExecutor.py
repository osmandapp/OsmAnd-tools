import concurrent
import queue
import threading
import time
from concurrent.futures import Future
from time import sleep
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
        self.semaphore.release()
        self.done_callback(future)
        with self.lock:
            del self.futures[future]

    def futures_timeout_args(self) -> Dict[Future, Tuple[float, Tuple]]:
        with self.lock:
            return dict(self.futures)

    def undone_futures_args(self, timeout: float = None) -> List[Tuple[float, Tuple]]:
        with self.lock:
            if timeout is None:
                return [v for k, v in self.futures.items() if not k.done()]
            else:
                return [v for k, v in self.futures.items() if not k.done() and time.time() - v[0] > timeout]


def task(arg1, arg2):
    print(f"#{threading.current_thread().name}. Processing: {(arg1, arg2)} ...")
    sleep(5)
    print(f"#{threading.current_thread().name}. Done: {(arg1, arg2)}")


def done_callback(future):
    pass


def main():
    tasks = [('id', 1), ('id', 2), ('id', 3), ('id', 4), ('id', 5), ('id', 6), ('id', 7), ('id', 8), ('id', 9), ('id', 10)]
    # Create a fixed-size blocking queue
    with BoundedThreadPoolExecutor(task, done_callback, 2, "Thread") as e:
        # Add tasks to the queue (blocks if queue is full)
        timeout = 2
        start_time = time.time()
        for i in tasks:
            print(f"Main: Attempting to add task {i}...")
            e.submit(*i)  # Blocks here if queue is full
            print(f"Main: Successfully added task {i}")
            if time.time() - start_time > timeout:
                print(f"Delayed tasks: {[arg2 for arg1, arg2 in e.undone_futures_args(timeout)]}.")


if __name__ == "__main__":
    main()
