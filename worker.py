import threading

class Worker(threading.Thread):
    """ A worker class
    """

    def __init__(self, target, args=(), kwargs={}, callback=None):
        # ... Thread init ...
        threading.Thread.__init__(self, daemon=True)
        # The shutdown_flag is a threading.Event object that
        # indicates whether the thread should be terminated.
        self._shutdown_flag = threading.Event()
        self._target = target
        self._args = args
        self._kwargs = kwargs
        self._callback = callback

    def run(self):
        """ Run worker loop
        """
        print(f"Thread #{self.ident} started")

        try:
            while not self._shutdown_flag.is_set():
                result = self._target(*self._args, **self._kwargs)

                if self._callback and result:
                    self._callback(result)
        finally:
            del self._target, self._args, self._kwargs, self._callback

        print(f"Thread #{self.ident} stopped")

    def stop(self):
        """ Send a shutdown flag
        """
        self._shutdown_flag.set()


class Pool(object):
    """ A pool class
    """

    def __init__(self, size, target, args=(), kwargs={}, callback=None):
        self._workers = []

        for _ in range(int(size)):
            worker = Worker(target, args, kwargs, callback)
            self._workers.append(worker)

    def start(self):
        for worker in self._workers:
            worker.start()

    def stop(self):
        for worker in self._workers:
            worker.stop()

    def join(self, timeout=None):
        for worker in self._workers:
            worker.join(timeout)

    def is_alive(self):
        return any([w.is_alive() for w in self._workers])
