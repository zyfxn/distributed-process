import math
import time
import vthread

from multiprocessing import Process, Queue, Value
from queue import Empty
from typing import Callable, Any
from singleton3 import Singleton


class Task(object):
    _func: Callable
    _args: Any

    def __init__(self, func: Callable, args: Any):
        self._func = func
        self._args = args

    @vthread.pool(pool_num=4)
    def run(self, stress: Value):
        self._func(self._args)
        stress.value -= 1


class WorkerProcess(Process):
    _running: Value
    _queue: Queue
    _stress: Value
    _received: Value

    def __init__(self, running_flag: Value):
        Process.__init__(self)
        self._running = running_flag
        self._queue = Queue()
        self._stress = Value('i', 0)
        self._received = Value('i', 0)

    def put_task(self, task: Task):
        self._queue.put(task)

    def get_stress(self):
        return self._stress.value

    def get_received_task_count(self):
        return self._received.value

    def get_qsize(self):
        return self._queue.qsize()

    def run(self):
        while self._running.value:
            try:
                task_data: Task = self._queue.get(block=False)
                self._received.value += 1
                self._stress.value += 1
                task_data.run(self._stress)
            except Empty:
                time.sleep(0.001)


class MasterProcess(Process):
    _workers: WorkerProcess = []
    _running: Value
    _queue: Queue
    _max_process_count: Value
    _task_limit_per_sec: int = 500

    def __init__(self):
        Process.__init__(self)
        self._running = Value('b', 1)
        self._queue = Queue()
        self._max_process_count = Value('i', 0)

    def set_max_process_count(self, count):
        self._max_process_count.value = count

    def add_process(self):
        worker = WorkerProcess(running_flag=self._running)
        self._workers.append(worker)
        worker.start()

    def put_task(self, task: Task):
        self._queue.put(task)

    def stop(self):
        self._running.value = 0

    def __get_worker_index(self, index):
        index += 1
        if index >= len(self._workers):
            index = 0
        return index

    def _check_workers_overload(self, index):
        remain_task_count_sum = 0
        for worker in self._workers:
            remain = worker.get_stress()
            if remain == 0:
                return index
            remain_task_count_sum += remain

        print("remain task:", remain_task_count_sum)
        need_process_count = math.ceil(self._task_limit_per_sec / (self._task_limit_per_sec - remain_task_count_sum))
        if need_process_count > self._max_process_count.value:
            need_process_count = self._max_process_count.value

        old_count = len(self._workers)
        add = need_process_count - old_count
        if add == 0:
            return index

        print("add process:", add)
        for i in range(add):
            self.add_process()
        return index

    def run(self):
        self.add_process()

        cur_time = time.time()
        task_counter = 0
        index = -1  # idle worker is unknown
        while self._running.value:
            if time.time() - cur_time > 1:
                index = self._check_workers_overload(index)
                cur_time = time.time()
                task_counter = 0

            if task_counter >= self._task_limit_per_sec:
                time.sleep(0.01)
                continue

            try:
                index = self.__get_worker_index(index)
                task: Task = self._queue.get(block=False)
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
    _master: MasterProcess
    _queue: Queue

    def __init__(self):
        self._master = MasterProcess()
        self._queue = Queue()
        self._master.start()

    def put_task(self, func: Callable, args: Any):
        task = Task(func=func, args=args)
        self._master.put_task(task)

    def shutdown(self):
        self._master.stop()
        self._master.join()

    def set_max_process_count(self, count):
        self._master.set_max_process_count(count)
