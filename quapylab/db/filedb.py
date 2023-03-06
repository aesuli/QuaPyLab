import json
from pathlib import Path

import dill
import shortuuid

from quapylab.db.quapydb import QuaPyDB, JobStatus
from quapylab.util import datetime_now_to_filename

USER_DATASET_GROUP = 'User data'

MODEL_FILE = 'model.dill'


class FileDB(QuaPyDB):

    def __init__(self, path):
        self._path = Path(path)
        if not self._path.exists():
            self._path.mkdir(parents=True, exist_ok=True)

        self._dataset_dir = self._path / 'user_datasets'
        if not self._dataset_dir.exists():
            self._dataset_dir.mkdir(parents=True, exist_ok=True)

        self._model_dir = self._path / 'models'
        if not self._model_dir.exists():
            self._model_dir.mkdir(parents=True, exist_ok=True)

        self._job_dir = self._path / 'jobs'
        if not self._job_dir.exists():
            self._job_dir.mkdir(parents=True, exist_ok=True)

        self._log_dir = self._path / 'logs'
        if not self._log_dir.exists():
            self._log_dir.mkdir(parents=True, exist_ok=True)

        users_file = self._path / 'user.json'
        if not users_file.exists() or users_file.stat().st_size == 0:
            with open(users_file, mode='wt', encoding='utf-8') as outputfile:
                json.dump({'admin': 'adminadmin'}, outputfile)

        with open(users_file, mode='rt', encoding='utf-8') as inputfile:
            self._users = json.load(inputfile)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def validate(self, username: str, password: str) -> bool:
        try:
            return self._users[username] == password
        except KeyError:
            return False

    def get_dataset_names(self):
        datasets = super().get_dataset_names()
        user_dataset = list()
        for subdir in self._dataset_dir.glob('*'):
            user_dataset.append(subdir)
        if USER_DATASET_GROUP not in datasets:
            datasets[USER_DATASET_GROUP] = user_dataset
        else:
            datasets[USER_DATASET_GROUP].extend(user_dataset)
        return datasets

    def set_quantifier(self, quantifier, experiment_name, model_name, overwrite=False):
        model_dir = self._model_dir / experiment_name / model_name
        if model_dir.exists():
            if not overwrite:
                raise FileExistsError(f'Quantifier "{experiment_name}/{model_name}" already exists.')
        else:
            model_dir.mkdir(parents=True, exist_ok=True)
        with open(model_dir / MODEL_FILE, mode='wb') as outputfile:
            dill.dump(quantifier, outputfile)

    def get_quantifier(self, experiment_name, model_name):
        with open(self._model_dir / experiment_name / model_name / MODEL_FILE, mode='rb') as inputfile:
            return dill.load(inputfile)

    def get_quantifier_names(self):
        quantifier_names = dict()
        for expdir in self._model_dir.glob('*'):
            if expdir.is_dir():
                for modeldir in expdir.glob('*'):
                    if modeldir.is_dir():
                        quantifier_names[expdir.name] = modeldir.name
        return quantifier_names

    def add_job(self, function, kwargs):
        job_id = f'{datetime_now_to_filename()}.{shortuuid.uuid()}'
        try:
            jobfile = self._job_dir / (f'{job_id}.{JobStatus.creating.value}')
            with open(jobfile, mode='wb') as outputfile:
                dill.dump((function, kwargs), outputfile)
            jobfile.rename(self._job_dir / (f'{job_id}.{JobStatus.pending.value}'))
        except Exception as e:
            e

    def pop_pending_job(self):
        try:
            job_filename = next(self._job_dir.glob(f'*.{JobStatus.pending.value}'))
            if job_filename is not None:
                job_id = job_filename.name[:-len(JobStatus.pending.value) - 1]
                new_filename = self._job_dir / f'{job_id}.{datetime_now_to_filename()}.{JobStatus.running.value}'
                job_filename.rename(new_filename)
                with open(new_filename, mode='rb') as inputfile:
                    function, kwargs = dill.load(inputfile)
                return job_id, function, kwargs
            return None, None, None
        except StopIteration:
            return None, None, None

    def job_done(self, job_id):
        job_filename = self._job_dir / next(self._job_dir.glob(f'{job_id}*'))
        new_filename = self._job_dir / f'{job_filename.name[:job_filename.name.rfind(".")]}.{datetime_now_to_filename()}.{JobStatus.done.value}'
        job_filename.rename(new_filename)

    def job_error(self, job_id):
        job_filename = self._job_dir / next(self._job_dir.glob(f'{job_id}*'))
        new_filename = self._job_dir / f'{job_filename.name[:job_filename.name.rfind(".")]}.{datetime_now_to_filename()}.{JobStatus.error.value}'
        job_filename.rename(new_filename)

    def log_error(self, job_id, msg, append=True):
        if append:
            mode = 'at'
        else:
            mode = 'wt'
        with open(self._log_dir / f'{job_id}.error_log.txt', mode=mode, encoding='utf-8') as outputfile:
            outputfile.write(msg)

    def job_list(self):
        return [job_file.name[:job_file.name.find('.',job_file.name.find('.')+1)] for job_file in self._job_dir.iterdir()]

    def job_info(self, job_id):
        job_filename = self._job_dir / next(self._job_dir.glob(f'{job_id}*'))
        fields = job_filename.name.split('.')
        status = fields[-1]
        created = fields[0]
        if len(fields) > 3:
            started = fields[2]
        else:
            started = 'n/a'
        if len(fields) > 4:
            completed = fields[3]
        else:
            completed = 'n/a'
        with open(job_filename, mode='rb') as inputfile:
            function, kwargs = dill.load(inputfile)
        return {'job_id': job_id, 'function': function.__name__, 'arguments': str(kwargs), 'status': status, 'created': created, 'started': started, 'completed': completed}

    def job_count(self):
        return len(list(self._job_dir.iterdir()))

    def job_delete(self, job_id):
        filename = next(self._job_dir.glob(f'{job_id}*'))
        filename.unlink(missing_ok=True)
        error_log_file = self._log_dir / f'{job_id}.error_log.txt'
        error_log_file.unlink(missing_ok=True)

    def job_rerun(self, job_id):
        filename = next(self._job_dir.glob(f'{job_id}*'))
        pending_filename = f'{filename.name[:filename.name.rfind(".")]}.{JobStatus.pending.value}'
        error_log_file = self._log_dir / f'{job_id}.error_log.txt'
        error_log_file.unlink(missing_ok=True)
        filename.rename(self._job_dir/pending_filename)

    def job_get_error_log(self, job_id):
        error_log_file =self._log_dir / f'{job_id}.error_log.txt'
        if not error_log_file.exists():
            return ''
        else:
            with open(error_log_file, mode='rt', encoding='utf-8') as inputfile:
                return inputfile.read()
