#!/usr/bin/env python3

import os
import time
import signal
import logging

from dotenv import load_dotenv

from pymongo import MongoClient
from mongoqueue import MongoQueue

from easysnmp import Session
from worker import Worker, ServiceExit

from helpers import print_snmp_variable

# loading .env file
load_dotenv()

# Init connection to MongoDB server and creaet DB
mongo = MongoClient(os.getenv('MONGO_URL'))
db = mongo[os.getenv('MONGO_DB')]

# Init queue in MongoDB server
queue = MongoQueue(db['tasks'], consumer_id=os.getenv('CONSUMER'))

# init logging config
logging.basicConfig(filename='messages.log',
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

def service_shutdown(signum, frame):
    """ Shutdown when caught signal
    """
    print(f'\nCaught signal {signum}')
    raise ServiceExit

def get_job():
    """ Find a next job
    """
    while True:
        job = queue.next()
        
        if job is None:
            # print('There is no job, go sleep :(')
            time.sleep(0.5)
        else:
            yield job

def do_work(job):
    """ Do the work
    """
    try:
        logging.info(f'Job id {job.job_id} is started')
        
        start_time = time.time()
        
        session = Session(hostname=job.payload['hostname'],
                          community=job.payload['community'],
                          version=2,
                          retries=1,
                          timeout=3)
        
        for item in session.walk(job.payload['oids']):
            # print_snmp_variable(item)
            pass

    except Exception as err:
        logging.error(f'Job id {job.job_id} ended with: {err}')
        job.error(message=str(err))
    else:
        logging.info(f'Job id {job.job_id} is complete in {time.time()-start_time:.2f} sec')            
        job.complete()
    finally:
        print(f'Jobs left: {queue.size()}', end='\r')

def main():
    """ Main program
    """
    # Register the signal handlers
    signal.signal(signal.SIGTERM, service_shutdown)
    signal.signal(signal.SIGINT, service_shutdown)

    print('Starting main program')

    # Init workers
    threads = [Worker(get_job, do_work) for w in range(int(os.getenv('THREADS')))]

    try:
        # Start the job threads
        for t in threads:
            t.start()
        # Keep the main thread running,
        # otherwise signals are ignored.
        while True:
            time.sleep(0.5)
    except ServiceExit:
        # Terminate the running threads.
        # Set the shutdown flag on each thread to
        # trigger a clean shutdown of each thread.
        for t in threads:
            t.stop()
            t.join()
    finally:
        mongo.close()
        
    print('Stopping main program')

if __name__ == '__main__':
    main()
