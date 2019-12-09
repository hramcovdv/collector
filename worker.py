import threading

from time import sleep

class Worker(threading.Thread):
    def __init__(self, do_work, get_job):
        # ... Thread init ...
        threading.Thread.__init__(self)

        # The shutdown_flag is a threading.Event object that
        # indicates whether the thread should be terminated.
        self.shutdown_flag = threading.Event()

        # ... Other thread setup ...
        self.do_work = do_work
        self.get_job = get_job
        self.current_job = None

    def run(self):
        print('Thread #%s started' % self.ident)
        while not self.shutdown_flag.is_set():
            self.do_work(self.get_job())
        print('Thread #%s stopped' % self.ident)

    def stop(self):
        self.shutdown_flag.set()

class ServiceExit(Exception):
    """
    Custom exception which is used to trigger the clean exit
    of all running threads and the main program.
    """
    pass
