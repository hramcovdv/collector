#!/usr/bin/env python3

import os
import time
import yaml
import signal
import logging
from dotenv import load_dotenv
from redis import Redis
from redisqueue import RedisQueue
from easysnmp import Session, EasySNMPError

from worker import Worker

# loading .env file
load_dotenv()

# Init redis connection
memo = Redis(
    host=os.getenv('REDIS_IP'),
    port=os.getenv('REDIS_PORT'),
    db=os.getenv('REDIS_DB')
    )

# init redis queue
queue = RedisQueue(
    connection=memo,
    name=os.getenv('QUEUE')
    )

# init logging config
logging.basicConfig(
    filename=os.getenv('LOGGING'),
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=getattr(logging, os.getenv('LOG_LEVEL').upper())
    )


class ServiceExit(Exception):
    """
    Custom exception which is used to trigger the clean exit
    of all running threads and the main program.
    """
    pass


def service_shutdown(signum, frame):
    """ Shutdown when caught signal
    """
    print(f"\nCaught signal {signum}")
    raise ServiceExit


def do_work():
    global queue

    item = queue.get(timeout=1)

    if item is None: return

    job = yaml.safe_load(item)

    try:
        start_time = time.time()

        session = Session(hostname=job['hostname'],
            community=job['community'],
            version=2,
            retries=1,
            timeout=5
            )

        for _ in session.walk(job['oids']):
            pass
    except EasySNMPError as error:
        logging.error(f"Job on {job['hostname']} ended with error: {error}")
    except Exception as error:
        logging.error(f"Job ended with error: {error}")
    else:
        logging.info(f"Job was complete in {time.time()-start_time:.2f} sec")


def statistics(interval=1):
    """ Statistic generator
    """
    global queue

    previous_count = 0
    while True:
        current_count = queue.size()
        performance = round((previous_count - current_count) / interval)

        yield f"Jobs left: {current_count}, Performance: {performance} jobs/sec"

        time.sleep(interval)
        previous_count = current_count


def main():
    """ Main service
    """
    # Register the signal handlers
    signal.signal(signal.SIGTERM, service_shutdown)
    signal.signal(signal.SIGINT, service_shutdown)

    print("Starting main service")

    # Init status generator
    status = statistics()

    # Init workers
    workers = []
    for _ in range(int(os.getenv('THREADS'))):
        worker = Worker(target=do_work, daemon=True)
        worker.start()
        workers.append(worker)

    try:
        # Keep the main thread running,
        # otherwise signals are ignored.
        while any([w.is_alive() for w in workers]):
            print("{:<80}".format(next(status)), end="\r")
    except ServiceExit:
        # Terminate the running threads.
        # Set the shutdown flag on each thread to
        # trigger a clean shutdown of each thread.
        for worker in workers:
            worker.stop()
            worker.join()
    except Exception as error:
        print(f"Main service ended with error: {error}")

    print("Stopping main service")


if __name__ == "__main__":
    main()
