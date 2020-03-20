import threading

class Worker(threading.Thread):
    """ A worker class
    """
    def __init__(self, do_work):
        # ... Thread init ...
        threading.Thread.__init__(self, daemon=True)
        # The shutdown_flag is a threading.Event object that
        # indicates whether the thread should be terminated.
        self._shutdown_flag = threading.Event()

        self._do_work = do_work

    def run(self):
        """ Run work loop
        """
        print(f'Thread #{self.ident} started')
        
        try:
            while not self._shutdown_flag.is_set():
                if self._do_work:
                    self._do_work(*self._args, **self._kwargs)
        finally:
            # Avoid a refcycle if the thread is running a function with
            # an argument that has a member that points to the thread.
            del self._do_work, self._args, self._kwargs

        print(f'Thread #{self.ident} stopped')

    def stop(self):
        """ Send a shutdown flag to the work loop
        """
        self._shutdown_flag.set()
