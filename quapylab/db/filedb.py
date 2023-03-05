import json
from pathlib import Path

import dill
import shortuuid

from quapylab.db.quapydb import QuaPyDB
from quapylab.util import datetime_now_to_filename

USER_DATASET_GROUP = 'User data'

MODEL_FILE = 'model.dill'


class JobStatus:
    creating = '.creating'
    pending = '.pending'
    running = '.running'
    done = '.done'
    error = '.error'


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
        job_name = f'{datetime_now_to_filename()}.{shortuuid.uuid()}'
        try:
            jobfile = self._job_dir / (job_name + JobStatus.creating)
            with open(jobfile, mode='wb') as outputfile:
                dill.dump((function, kwargs), outputfile)
            jobfile.rename(self._job_dir / (job_name + JobStatus.pending))
        except Exception as e:
            e

    def pop_pending_job(self):
        try:
            job_filename = next(self._job_dir.glob(f'*{JobStatus.pending}'))
            if job_filename is not None:
                job_name = job_filename.name[:-len(JobStatus.pending)]
                new_filename = self._job_dir / f'{job_name}.{datetime_now_to_filename()}{JobStatus.running}'
                job_filename.rename(new_filename)
                with open(new_filename, mode='rb') as inputfile:
                    function, kwargs = dill.load(inputfile)
                return job_name, function, kwargs
            return None, None, None
        except StopIteration:
            return None, None, None

    def job_done(self, job_name):
        job_filename = self._job_dir / next(self._job_dir.glob(f'{job_name}*{JobStatus.running}'))
        running_job_name = job_filename.name[:-len(JobStatus.running)]
        new_filename = self._job_dir / f'{running_job_name}.{datetime_now_to_filename()}{JobStatus.done}'
        job_filename.rename(new_filename)

    def job_error(self, job_name):
        job_filename = self._job_dir / next(self._job_dir.glob(f'{job_name}*{JobStatus.running}'))
        running_job_name = job_filename.name[:-len(JobStatus.running)]
        new_filename = self._job_dir / f'{running_job_name}.{datetime_now_to_filename()}{JobStatus.error}'
        job_filename.rename(new_filename)

    def log_error(self, job_name, msg):
        with open(self._job_dir / f'{job_name}.error.txt', mode='wt',
                  encoding='utf-8') as outputfile:
            outputfile.write(msg)