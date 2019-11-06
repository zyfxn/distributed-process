import time

from distributed_process import ProcessService


def hello(arg):
    time.sleep(0.01)


if __name__ == '__main__':
    service = ProcessService()
    service.startup()

    for i in range(30):
        service.put_task(func=hello, args=0)

    time.sleep(1)
    service.shutdown()
