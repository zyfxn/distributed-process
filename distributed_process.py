import time
import vthread

from multiprocessing import Process, Queue, Value
from queue import Empty
from typing import Callable, Any

g_thread_count = 1
g_max_process_count = 3


class Task(object):
    _func: Callable
    _args: Any

    def __init__(self, func: Callable, args: Any):
        self._func = func
        self._args = args

    @vthread.pool(g_thread_count)
    def run(self, stress: Value):
        self._func(self._args)
        stress.value -= 1


class ProcessWorker(Process):
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

    def run(self):
        while self._running.value:
            try:
                task_data: Task = self._queue.get(block=False)
                self._received.value += 1
                task_data.run(self._stress)
            except Empty:
                time.sleep(0.001)


class ProcessMaster(Process):
    _workers: ProcessWorker = []
    _running: Value
    _queue: Queue

    def __init__(self):
        Process.__init__(self)
        self._running = Value('b', 1)
        self._queue = Queue()

    def add_process(self):
        worker = ProcessWorker(running_flag=self._running)
        self._workers.append(worker)
        worker.start()

    def put_task(self, task: Task):
        self._queue.put(task)

    def stop(self):
        self._running.value = 0

    def __get_idle_worker_index(self, index):
        count = len(self._workers)
        if count == 0:
            return -1

        index = self.__next_worker(index, count)
        overload = 0
        while self._workers[index].get_stress() >= g_thread_count:
            overload += 1
            if overload >= count:
                return -1
            index = self.__next_worker(index, count)

        return index

    @staticmethod
    def __next_worker(index, count):
        index += 1
        if index >= count:
            index = 0
        return index

    def __wait_for_idle_worker_index(self):
        index = -1
        while index < 0 and self._running.value:
            index = self.__get_idle_worker_index(0)
            time.sleep(0.001)
        return index

    def run(self):
        self.add_process()
        idle_worker_index = -1    # idle worker is unknown
        while self._running.value:
            try:
                idle_worker_index = self.__get_idle_worker_index(idle_worker_index)
                if idle_worker_index < 0:
                    if len(self._workers) < g_max_process_count:
                        self.add_process()
                        idle_worker_index = len(self._workers) - 1
                    else:
                        idle_worker_index = self.__wait_for_idle_worker_index()

                if not self._running.value:
                    break

                task: Task = self._queue.get(block=False)
                self._workers[idle_worker_index].put_task(task)

            except Empty:
                time.sleep(0.001)

        for worker in self._workers:
            worker.join()
            print(worker.get_received_task_count())


class ProcessService(object):
    _master: ProcessMaster
    _queue: Queue

    def __init__(self):
        self._master = ProcessMaster()
        self._queue = Queue()

    def put_task(self, func: Callable, args: Any):
        task = Task(func=func, args=args)
        self._master.put_task(task)

    def startup(self):
        self._master.start()

    def shutdown(self):
        self._master.stop()
