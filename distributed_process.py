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

    @vthread.pool(4)
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
        self._stress.value += 1

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
                task_data.run(self._stress)
            except Empty:
                time.sleep(0.001)


class MonitorProcess(Process):
    pass


class MasterProcess(Process):
    _workers: WorkerProcess = []
    _running: Value
    _queue: Queue
    _max_process_count: Value
    _queuing_worker_count: int = 0

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
        count = len(self._workers)
        if count == 0:
            return -1

        index = self.__next_worker(index, count)
        if self._workers[index].get_qsize() > 0:
            self._queuing_worker_count += 1

        return index

    def __next_worker(self, index, count):
        index += 1
        if index >= count:
            index = 0
            self._queuing_worker_count = 0
        return index

    def __wait_for_idle_worker_index(self):
        index = -1
        while index < 0 and self._running.value:
            index = self.__get_worker_index(0)
            time.sleep(0)
        return index

    def run(self):
        self.add_process()
        idle_worker_index = -1  # idle worker is unknown
        while self._running.value:
            try:
                idle_worker_index = self.__get_worker_index(idle_worker_index)

                worker_count = len(self._workers)
                if self._queuing_worker_count >= worker_count and \
                        worker_count < self._max_process_count.value:
                    self.add_process()
                    idle_worker_index = len(self._workers) - 1

                task: Task = self._queue.get(block=False)
                self._workers[idle_worker_index].put_task(task)

            except Empty:
                time.sleep(0.001)

        print("data in the queue remain", self._queue.qsize())
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
