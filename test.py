import time

from dtbprocess import ProcessService


def hello(arg):
    time.sleep(0.01)


if __name__ == '__main__':
    service = ProcessService()

    start_time = time.time()
    for i in range(3000):
        service.put(hello, 0)
    service.shutdown()
    print("shutdown,", str(time.time() - start_time))
