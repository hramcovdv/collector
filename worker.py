import threading

class Worker(threading.Thread):
    """ A worker class
    """
    
    def __init__(self, get_job, do_work):
        # ... Thread init ...
        threading.Thread.__init__(self)

        # The shutdown_flag is a threading.Event object that
        # indicates whether the thread should be terminated.
        self.shutdown_flag = threading.Event()

        # Generator that get the job
        self.get_job = get_job
        
        # Function that run the work
        self.do_work = do_work

    def run(self):
        """ Run work loop
        """
        print('Thread #%s started' % self.ident)
        
        # Init job generator
        job = self.get_job()
        
        while not self.shutdown_flag.is_set():
            self.do_work(next(job))
            
        print('Thread #%s stopped' % self.ident)

    def stop(self):
        """ Send a shutdown flag to the work loop
        """
        self.shutdown_flag.set()

class ServiceExit(Exception):
    """
    Custom exception which is used to trigger the clean exit
    of all running threads and the main program.
    """
    pass
