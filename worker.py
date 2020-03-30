import threading

class Worker(threading.Thread):
    """ A worker class
    """

    def __init__(self, *args, **kwargs):
        # ... Thread init ...
        threading.Thread.__init__(self, *args, **kwargs)
        # The shutdown_flag is a threading.Event object that
        # indicates whether the thread should be terminated.
        self._shutdown_flag = threading.Event()

    def run(self):
        """ Run worker loop
        """
        print(f"Thread #{self.ident} started")

        try:
            while not self._shutdown_flag.is_set():
                if self._target:
                    self._target(*self._args, **self._kwargs)
        finally:
            del self._target, self._args, self._kwargs

        print(f"Thread #{self.ident} stopped")

    def stop(self):
        """ Send a shutdown flag
        """
        self._shutdown_flag.set()
