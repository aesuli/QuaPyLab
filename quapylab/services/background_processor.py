import logging
import multiprocessing
import signal
import traceback
from functools import partial
from multiprocessing import BoundedSemaphore, Process
from multiprocessing.pool import Pool
from time import sleep

import cherrypy

__author__ = 'Andrea Esuli'

from quapylab.db.filedb import FileDB

LOOP_WAIT = 1  # second


def setup_background_processor_log(**kwargs):
    #    logging.basicConfig(encoding='utf-8', stream=sys.stderr, level=logging.INFO)
    pass


class JobError:
    def __init__(self, name, exception, tb):
        self.name = name
        self.e = str(exception)
        self.tb = tb

    def __str__(self):
        return f'{self.__class__.__name__}(\'{self.name}\', \'{self.e}\')\n{self.tb}'


process_db = None


def job_launcher(job_name, f, **kwargs):
    global process_db
    try:
        f(db=process_db, **kwargs)
    except Exception as e:
        return JobError(job_name, e, traceback.format_exc())


def bp_pool_initializer(db_connection_string, initializer, *initargs):
    cherrypy.log(f'BackgroundProcessor: adding {multiprocessing.current_process().name} to pool', severity=logging.INFO)
    global process_db
    process_db = FileDB(db_connection_string)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    if initializer is not None:
        initializer(*initargs)


class BackgroundProcessor(Process):
    def __init__(self, db_connection_string, pool_size, initializer=None, initargs=None):
        Process.__init__(self)
        self._stop_event = multiprocessing.Event()
        self._pool_size = pool_size
        self._db_connection_string = db_connection_string
        self._initializer = partial(bp_pool_initializer, db_connection_string, initializer)
        if initargs is None:
            initargs = []
        self._initargs = initargs
        self._running = False
        self._semaphore = BoundedSemaphore(self._pool_size)

    def run(self):
        with FileDB(self._db_connection_string) as db, \
                Pool(processes=self._pool_size, initializer=self._initializer, initargs=self._initargs) as pool:
            cherrypy.log('BackgroundProcessor: started', severity=logging.INFO)
            while not self._stop_event.is_set():
                try:
                    job_name, function, kwargs = db.pop_pending_job()
                except Exception as e:
                    cherrypy.log(
                        f'Error fetching next job \nException: {e}',
                        severity=logging.ERROR)
                    job_name = None
                if job_name is None:
                    try:
                        sleep(LOOP_WAIT)
                    finally:
                        continue
                self._semaphore.acquire()
                try:
                    cherrypy.log(f'Starting {job_name}: {function} ({kwargs})', severity=logging.INFO)
                    pool.apply_async(partial(job_launcher, job_name, function), kwds=kwargs,
                                             callback=partial(self._release, db, job_name, True),
                                             error_callback=partial(self._release, db, job_name, False))
                except Exception as e:
                    self._semaphore.release()
                    cherrypy.log(f'Error on job {job_name}:\nException: ' + str(e),
                                 severity=logging.ERROR)
            pool.close()
            pool.join()
            cherrypy.log('BackgroundProcessor: stopped', severity=logging.INFO)

    def stop(self):
        self._stop_event.set()
        cherrypy.log('BackgroundProcessor: stopping')
        self.join()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()
        return False

    def _release(self, db, name, success, return_value=None):
        try:
            if not success:
                cherrypy.log(return_value, severity=logging.ERROR)
            else:
                cherrypy.log(f'Completed {name}', severity=logging.INFO)
            if hasattr(return_value, 're_raise'):
                return_value.re_raise()
        finally:
            self._semaphore.release()
