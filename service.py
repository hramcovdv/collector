#!/usr/bin/env python3

import os
import time
import yaml
import signal
import logging
from dotenv import load_dotenv
from redis import Redis
from redisqueue import RedisQueue
from easysnmp import Session

from worker import Worker

# loading .env file
load_dotenv()

# Init redis connection
memo = Redis(host=os.getenv('REDIS_IP'),
             port=os.getenv('REDIS_PORT'),
             db=os.getenv('REDIS_DB'))

# init redis queue
queue = RedisQueue(connection=memo,
                   name=os.getenv('QUEUE'))

# init logging config
logging.basicConfig(filename=os.getenv('LOGGING'),
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.WARNING)

class ServiceExit(Exception):
    """
    Custom exception which is used to trigger the clean exit
    of all running threads and the main program.
    """
    pass

def service_shutdown(signum, frame):
    """ Shutdown when caught signal
    """
    print(f'\nCaught signal {signum}')
    raise ServiceExit

def do_work():
    global queue
    
    job = queue.get(timeout=1)

    if job is None:
        return
    
    job = yaml.safe_load(job)

    try:
        start_time = time.time()

        session = Session(hostname=job['hostname'],
                          community=job['community'],
                          version=2,
                          retries=1,
                          timeout=3)

        for item in session.walk(job['oids']):
            pass
    except Exception as err:
        logging.error(f'Job ended with: {err}')
    else:
        logging.info(f'Job is complete in {time.time()-start_time:.2f} sec')            

def main():
    """ Main service
    """
    # Register the signal handlers
    signal.signal(signal.SIGTERM, service_shutdown)
    signal.signal(signal.SIGINT, service_shutdown)

    print('Starting main service')

    # Init workers
    workers = []
        
    for _ in range(int(os.getenv('THREADS'))):
        worker = Worker(do_work)
        worker.start()
        workers.append(worker)

    try:
        # Keep the main thread running,
        # otherwise signals are ignored.
        while any([w.is_alive() for w in workers]):
            print(f'Jobs left: {queue.size():>10d}', end='\r')
            time.sleep(0.5)
    except ServiceExit:
        # Terminate the running threads.
        # Set the shutdown flag on each thread to
        # trigger a clean shutdown of each thread.
        for worker in workers:
            worker.stop()
            worker.join()
    except Exception as err:
        print(f'Main service ended with: {err}')

    print('Stopping main service')

if __name__ == '__main__':
    main()
