import time

from distributed_process import ProcessService


def hello(arg):
    time.sleep(0.01)


if __name__ == '__main__':
    service = ProcessService()
    service.set_max_process_count(10)

    start_time = time.time()
    for i in range(1000):
        service.put_task(func=hello, args=0)

    time.sleep(3)
    service.shutdown()
