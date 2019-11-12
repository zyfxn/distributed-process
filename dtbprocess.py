import math
import time

from multiprocessing import Process, Queue, Value
from threading import Thread
from typing import Callable, Any
from singleton3 import Singleton


def stopcall(self):
    pass


class _WorkerThread(Thread):
    def __init__(self, queue: Queue):
        Thread.__init__(self)
        self._queue = queue
        self.done = 0

    def put(self, v):
        self._queue.put(v)

    def run(self):
        while True:
            v = self._queue.get()
            if v == stopcall:
                break
            try:
                func = v
                func(self)
            except BaseException as e:
                print(" - thread stop_by_error - ", e)
            self.done += 1


class _WorkerProcess(Process):
    def __init__(self, thread_count: int):
        Process.__init__(self)
        self._queue = Queue()
        self._thread_count = thread_count
        self._threads = []
        self._running = Value('b', 1)

    def put(self, v):
        if v == stopcall:
            self._running.value = 0
        self._queue.put(v)

    def get_stress(self):
        return self._queue.qsize()

    def poll(self, out: Queue):
        while not self._queue.empty():
            out.put(self._queue.get())

    def run(self):
        for _ in range(self._thread_count):
            worker = _WorkerThread(self._queue)
            worker.start()
            self._threads.append(worker)

        while self._running.value:
            time.sleep(0.01)

        for _ in range(self._thread_count - 1):
            self._queue.put(stopcall)


class _MasterProcess(Process):
    def __init__(self):
        Process.__init__(self)
        self._queue = Queue()
        self._queue_redistribute = Queue()
        self._max_process_count = 10
        self._task_limit_per_sec = 500
        self._workers: _WorkerProcess = []

    def _add_process(self):
        worker = _WorkerProcess(4)
        self._workers.append(worker)
        worker.start()

    def put(self, task):
        self._queue.put(task)

    def stop(self):
        self._queue.put(stopcall)

    def _get_worker_index(self, index):
        if index < 0:
            return 0

        index += 1
        if index >= len(self._workers):
            index = 0
        return index

    def _check_workers_stress(self):
        remain_task_count_sum = 0
        for worker in self._workers:
            remain = worker.get_stress()
            if remain == 0:
                return
            remain_task_count_sum += remain

        print(self._task_limit_per_sec, "task limit per sec, remain", remain_task_count_sum)
        current_process_count = len(self._workers)
        need_process_count = math.ceil(self._task_limit_per_sec / (self._task_limit_per_sec - remain_task_count_sum))
        if need_process_count + current_process_count >= self._max_process_count:
            print("reach max process limit")
            return

        for worker in self._workers:
            worker.poll(self._queue_redistribute)

        print("add", need_process_count, "process. redistribute tasks", self._queue_redistribute.qsize())
        for i in range(need_process_count):
            self._add_process()

        index = -1
        while not self._queue_redistribute.empty():
            index = self._get_worker_index(index)
            self._workers[index].put(self._queue_redistribute.get())

    def run(self):
        self._add_process()

        cur_time = time.time()
        task_counter = 0
        index = -1  # idle worker is unknown
        while True:
            if time.time() - cur_time > 1:
                self._check_workers_stress()
                cur_time = time.time()
                task_counter = 0

            if task_counter >= self._task_limit_per_sec:
                time.sleep(0.01)
                continue

            index = self._get_worker_index(index)
            v = self._queue.get()
            if v == stopcall:
                break
            task_counter += 1
            self._workers[index].put(v)

        print("stop, clear remain data", self._queue.qsize())
        while not self._queue.empty():
            self._queue.get()
        for worker in self._workers:
            worker.put(stopcall)


class ProcessService(object, metaclass=Singleton):
    def __init__(self):
        self._queue = Queue()
        self._master = _MasterProcess()
        self._master.start()

    def put(self, func: Callable, args: Any):
        self._master.put(func)

    def shutdown(self):
        self._master.stop()
        self._master.join()
