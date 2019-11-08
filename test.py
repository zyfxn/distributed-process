import time

from dtbprocess import ProcessService


def hello(arg):
    time.sleep(0.01)


if __name__ == '__main__':
    service = ProcessService()

    start_time = time.time()
    for i in range(1000):
        service.put_task(func=hello, args=0)

    time.sleep(3)
    service.shutdown()
    print("shutdown,", str(time.time() - start_time))
