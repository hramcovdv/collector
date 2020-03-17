import time
import logging
import threading
from easysnmp import Session
from mongoqueue import MongoQueue

class Worker(threading.Thread):
    """ A worker class
    """
    def __init__(self, database):
        # ... Thread init ...
        threading.Thread.__init__(self, daemon=True)

        # The shutdown_flag is a threading.Event object that
        # indicates whether the thread should be terminated.
        self.shutdown_flag = threading.Event()
        
        # Init queue in MongoDB server
        self.queue = MongoQueue(collection=database.tasks,
                                consumer_id=self.ident,
                                max_attempts=1)

    def run(self):
        """ Run work loop
        """
        print(f'Thread #{self.ident} started')
        
        while not self.shutdown_flag.is_set():
            job = self.queue.next()
            
            if job is None:
                time.sleep(0.5)
                continue

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
            finally:
                print(f'Jobs left: {queue.size()}', end='\r')
            
        print(f'Thread #{self.ident} stopped')

    def stop(self):
        """ Send a shutdown flag to the work loop
        """
        self.shutdown_flag.set()
