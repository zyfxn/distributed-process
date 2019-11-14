import time

from dtbprocess import ProcessService


def hello(a, b, c):
    time.sleep(0.01)


if __name__ == '__main__':
    service = ProcessService()
    service.config(task_limit_per_sec=1500)
    service.start()

    start_time = time.time()
    for i in range(5000):
        service.put(hello, 1, 2, 3)
    service.shutdown()
    print("shutdown,", str(time.time() - start_time))
