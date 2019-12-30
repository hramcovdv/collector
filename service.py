#!/usr/bin/env python3

import os
import signal

from time import sleep
from dotenv import load_dotenv

from pymongo import MongoClient
from mongoqueue import MongoQueue

from influxdb import InfluxDBClient

from easysnmp import Session
from worker import Worker, ServiceExit

from transform import get_point, get_points

# loading .env file
load_dotenv()

# Init connection to MongoDB server
mongo = MongoClient('mongodb://%s:%s@localhost' % ('root', 'root'))
# Init queue in MongoDB server
queue = MongoQueue(mongo.collector.tasks, consumer_id=os.getenv('CONSUMER'))
# Init connection to InfluxDB
influx = InfluxDBClient('localhost', 8086, 'root', 'root', 'collector')

def service_shutdown(signum, frame):
    """ Shutdown when caught signal
    """
    print('\nCaught signal %d' % signum)
    raise ServiceExit

def get_job():
    """ Find a job or take a break
    """
    job = queue.next()
    if job is None:
        sleep(0.5)
    return job

def do_work(job):
    """ Do the work if there is
    """
    if not job is None:
        try:
            # Job code here
            print('Job #%s is started' % str(job.job_id))
            # Init SNMP session
            snmp = Session(hostname=job.payload['hostname'],
                           community=job.payload['community'],
                           version=2)

            tags = job.payload.setdefault('tags', {})

            # Lets walk...
            for oid in job.payload['oids']:
                points = get_point(snmp.get(oid)) or get_points(snmp.walk(oid))
                points = [points] if type(points) is not list else points
                influx.write_points(points, tags=tags, time_precision='s')

            # Job is done
            job.complete()
        except Exception as err:
            print('Job #%s ended with an error: %s' % (str(job.job_id), str(err)))
            job.error(message=str(err))

def main():
    """ Main program
    """
    # Register the signal handlers
    signal.signal(signal.SIGTERM, service_shutdown)
    signal.signal(signal.SIGINT, service_shutdown)

    print('Starting main program')

    # Init workers
    threads = [Worker(do_work, get_job) for w in range(int(os.getenv('THREADS')))]

    try:
        # Start the job threads
        for t in threads:
            t.start()
        # Keep the main thread running,
        # otherwise signals are ignored.
        while True:
            sleep(0.5)
    except ServiceExit:
        # Terminate the running threads.
        # Set the shutdown flag on each thread to
        # trigger a clean shutdown of each thread.
        for t in threads:
            t.stop()
            t.join()

if __name__ == '__main__':
    main()
