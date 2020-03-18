import time
import logging
import threading
from easysnmp import Session

class Worker(threading.Thread):
    """ A worker class
    """
    def __init__(self, queue, timeout=0.5):
        # ... Thread init ...
        threading.Thread.__init__(self, daemon=True)
        # The shutdown_flag is a threading.Event object that
        # indicates whether the thread should be terminated.
        self.shutdown_flag = threading.Event()
        # When there is no job, take a timeout
        self.timeout = timeout
        # Jobs queue
        self.queue = queue

    def run(self):
        """ Run work loop
        """
        print(f'Thread #{self.ident} started')

        while not self.shutdown_flag.is_set():
            job = self.queue.next()

            if job is None:
                # time.sleep(self.timeout)
                break

            try:
                start_time = time.time()

                logging.info(f'Job id-{job.job_id} is started')

                session = Session(hostname=job.payload['hostname'],
                                  community=job.payload['community'],
                                  version=2,
                                  retries=1,
                                  timeout=3)

                for item in session.walk(job.payload['oids']):
                    pass
            except Exception as err:
                logging.error(f'Job id-{job.job_id} ended with: {err}')
                job.error(message=str(err))
            else:
                logging.info(f'Job id-{job.job_id} is complete in {time.time()-start_time:.2f} sec')            
                job.complete()

        print(f'Thread #{self.ident} stopped')

    def stop(self):
        """ Send a shutdown flag to the work loop
        """
        self.shutdown_flag.set()
