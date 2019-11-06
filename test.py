import time

from distributed_process import ProcessService


def hello(arg):
    time.sleep(0.01)


if __name__ == '__main__':
    service = ProcessService()
    start_time = time.time()
    for i in range(200):
        service.put_task(func=hello, args=0)

    service = ProcessService()
    for i in range(100):
        service.put_task(func=hello, args=0)

    print("task put done, %0.6f" % (time.time() - start_time))
    hello(None)
    print("test sleep, %0.6f" % (time.time() - start_time))
    time.sleep(1)
    print("sleep then shutdown, %0.6f" % (time.time() - start_time))
    service.shutdown()
    print("finish, %0.6f" % (time.time() - start_time))
