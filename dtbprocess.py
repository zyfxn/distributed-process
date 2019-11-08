import math
import time
import vthread

from multiprocessing import Process, Queue, Value
from queue import Empty
from typing import Callable, Any
from singleton3 import Singleton


class _WorkerItem(object):
    _func: Callable
    _args: Any

    def __init__(self, func: Callable, args: Any):
        self._func = func
        self._args = args

    @vthread.pool(pool_num=4)
    def run(self, stress: Value):
        self._func(self._args)
        stress.value -= 1


class _WorkerProcess(Process):
    _running: Value
    _split: Value
    _queue: Queue
    _split_queue: Queue
    _stress: Value
    _received: Value

    def __init__(self, running_flag: Value, split_flow_flag: Value, split_queue: Queue):
        Process.__init__(self)
        self._running = running_flag
        self._split = split_flow_flag
        self._queue = Queue()
        self._split_queue = split_queue
        self._stress = Value('i', 0)
        self._received = Value('i', 0)

    def put_task(self, task: _WorkerItem):
        self._queue.put(task)

    def get_stress(self):
        return self._stress.value

    def get_received_task_count(self):
        return self._received.value

    def run(self):
        while self._running.value:
            try:
                if self._split.value:
                    v = vthread.pool.queue_get()
                    if v is None:
                        continue
                    self._split_queue.put(v)
                    continue
                task_data: _WorkerItem = self._queue.get(block=False)
                self._received.value += 1
                self._stress.value += 1
                task_data.run(self._stress)
            except Empty:
                time.sleep(0.001)


class _MasterProcess(Process):
    _workers: _WorkerProcess = []
    _running: Value
    _split: Value
    _queue: Queue
    _split_queue: Queue
    _max_process_count: Value
    _task_limit_per_sec: int

    def __init__(self):
        Process.__init__(self)
        self._running = Value('b', 1)
        self._split = Value('b', 0)
        self._queue = Queue()
        self._split_queue = Queue()
        self._max_process_count = Value('i', 10)
        self._task_limit_per_sec = 500

    def _add_process(self):
        worker = _WorkerProcess(running_flag=self._running, split_flow_flag=self._split, split_queue=self._split_queue)
        self._workers.append(worker)
        worker.start()

    def put_task(self, task: _WorkerItem):
        self._queue.put(task)

    def stop(self):
        self._running.value = 0

    def _get_worker_index(self, index):
        index += 1
        if index >= len(self._workers):
            index = 0
        return index

    def _check_workers_overload(self):
        remain_task_count_sum = 0
        for worker in self._workers:
            remain = worker.get_stress()
            if remain == 0:
                return
            remain_task_count_sum += remain

        print("remain task:", remain_task_count_sum)
        need_process_count = math.ceil(self._task_limit_per_sec / (self._task_limit_per_sec - remain_task_count_sum))
        if need_process_count > self._max_process_count.value:
            need_process_count = self._max_process_count.value

        old_count = len(self._workers)
        if need_process_count <= old_count:
            return
        add = need_process_count - old_count

        # self._split.value = 1  # start split data flow in thread pool

        print("add process:", add)
        for i in range(add):
            self._add_process()
        # print(self._split_queue.qsize())
        return

    def run(self):
        self._add_process()

        cur_time = time.time()
        task_counter = 0
        index = -1  # idle worker is unknown
        while self._running.value:
            if time.time() - cur_time > 1:
                self._check_workers_overload()
                cur_time = time.time()
                task_counter = 0

            if task_counter >= self._task_limit_per_sec:
                time.sleep(0.01)
                continue

            try:
                index = self._get_worker_index(index)
                task: _WorkerItem = self._queue.get(block=False)
                task_counter += 1
                self._workers[index].put_task(task)
            except Empty:
                time.sleep(0.01)

        print("stop, clear remain data", self._queue.qsize())
        while not self._queue.empty():
            self._queue.get()

        for worker in self._workers:
            worker.join()
            print(worker.get_received_task_count())


class ProcessService(object, metaclass=Singleton):
    _master: _MasterProcess
    _queue: Queue

    def __init__(self):
        self._master = _MasterProcess()
        self._queue = Queue()
        self._master.start()

    def put_task(self, func: Callable, args: Any):
        task = _WorkerItem(func=func, args=args)
        self._master.put_task(task)

    def shutdown(self):
        self._master.stop()
        self._master.join()
