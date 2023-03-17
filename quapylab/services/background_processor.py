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
from quapylab.db.quapydb import QuaPyDB

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


process_db: QuaPyDB = None


def job_function(f):
    covars = f.__code__.co_varnames
    must_have = ['db', 'job_id']
    for arg_name in must_have:
        if arg_name not in covars:
            raise AttributeError(f'Function must have a {arg_name} argument')
    return f


def job_launcher(job_id, f, **kwargs):
    global process_db
    try:
        kwargs['job_id'] = job_id
        kwargs['db'] = process_db
        f(**kwargs)
    except Exception as e:
        process_db.log_job_error(job_id, f'{e}\n{traceback.format_exc()}')
        return JobError(job_id, e, traceback.format_exc())


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
                    job_id, function, kwargs = db.pop_pending_job()
                except Exception as e:
                    cherrypy.log(
                        f'Error fetching next job \nException: {e}',
                        severity=logging.ERROR)
                    job_id = None
                if job_id is None:
                    try:
                        sleep(LOOP_WAIT)
                    finally:
                        continue
                self._semaphore.acquire()
                try:
                    cherrypy.log(f'Starting {job_id}: {function} ({kwargs})', severity=logging.INFO)
                    pool.apply_async(partial(job_launcher, job_id, function), kwds=kwargs,
                                     callback=partial(self._release, db, job_id, True),
                                     error_callback=partial(self._release, db, job_id, False))
                except Exception as e:
                    self._semaphore.release()
                    cherrypy.log(f'Error on job {job_id}:\nException: ' + str(e),
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

    def _release(self, db, job_id, success, return_value=None):
        try:
            if not success or isinstance(return_value, JobError):
                cherrypy.log(str(return_value), severity=logging.ERROR)
                db.set_job_failed(job_id)
            else:
                cherrypy.log(f'Completed {job_id}', severity=logging.INFO)
                db.set_job_done(job_id)
            if hasattr(return_value, 're_raise'):
                return_value.re_raise()
        finally:
            self._semaphore.release()
