#!/usr/bin/env python3

import os
import time
import signal
import logging
from dotenv import load_dotenv
from pymongo import MongoClient
from mongoqueue import MongoQueue

from worker import Worker

# loading .env file
load_dotenv()

# Init connection to MongoDB server and creaet DB
mongo = MongoClient(os.getenv('MONGO_URL'))
db = mongo[os.getenv('MONGO_DB')]

# Init queue in MongoDB server
queue = MongoQueue(collection=db.tasks,
                   consumer_id=os.getenv('CONSUMER'),
                   max_attempts=1)

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

def statistics(queue, interval=1):
    """ Statistic generator
    """
    previous_count = 0
    
    while True:
        current_count = queue.size()
        performance = round((previous_count - current_count) / interval)

        yield f'Jobs left: {current_count}, Performance: {performance} jobs/sec'

        time.sleep(interval)
        previous_count = current_count

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
        worker = Worker(queue)
        worker.start()
        workers.append(worker)

    try:
        # Repair locked jobs
        queue.repair()
        
        # Init status generator
        status = statistics(queue)
        
        # Keep the main thread running,
        # otherwise signals are ignored.
        while any([w.is_alive() for w in workers]):
            print('{:<80}'.format(next(status)), end='\r')
    except ServiceExit:
        # Terminate the running threads.
        # Set the shutdown flag on each thread to
        # trigger a clean shutdown of each thread.
        for worker in workers:
            worker.stop()
            worker.join()
    except Exception as err:
        print(f'Main service ended with: {err}')
    finally:
        mongo.close()

    print('Stopping main service')

if __name__ == '__main__':
    main()
