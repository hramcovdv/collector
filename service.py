#!/usr/bin/env python3

import os
import time
import signal

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

def service_shutdown(signum, frame):
    """ Shutdown when caught signal
    """
    print('\nCaught signal %d' % signum)
    raise ServiceExit

def get_job():
    """ Find a next job
    """
    while True:
        job = queue.next()
        
        if job is None:
            print('There is no job :(')
            time.sleep(0.5)
        else:
            yield job

def do_work(job):
    """ Do the work
    """
    try:
        print('Job id %s is started' % job.job_id)
        start_time = time.time()
        
        session = Session(hostname=job.payload['hostname'],
                          community=job.payload['community'],
                          version=2,
                          retries=1,
                          timeout=3)
        
        for item in session.walk(job.payload['oids']):
            print_snmp_variable(item)

    except Exception as err:
        print('Job id {0} ended with an error: {1}'.format(job.job_id, str(err)))
        job.error(message=str(err))
    else:
        print('Job id {0} is complete in {1:.2f} sec'.format(job.job_id,
                                                             time.time() - start_time))            
        job.complete()
    finally:
        print('Jobs left: %d' % queue.size())

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

if __name__ == '__main__':
    main()
