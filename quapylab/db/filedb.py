import datetime
import json
import shutil
from pathlib import Path

import dill
import pandas as pd
import shortuuid

from quapylab.db.quapydb import QuaPyDB, JobStatus, get_label_column_name, get_text_column_name, get_data_column_names
from quapylab.util import datetime_now_to_filename

DATASET_EXTENSION = '.dataset'
DATASET_INFO_EXTENSION = '.dataset_info'
QUANTIFIER_EXTENSION = '.quantifier'
LOG_EXTENSION = '.log'


def check_name(name):
    block_list = ['/', '\\', '..', '*', '?']
    for blocked in block_list:
        if blocked in name:
            raise ValueError(f'Dataset name cannot contain {blocked}')


class FileDB(QuaPyDB):

    def __init__(self, path):
        self._path = Path(path)
        if not self._path.exists():
            self._path.mkdir(parents=True, exist_ok=True)

        self._dataset_dir = self._path / 'datasets'
        if not self._dataset_dir.exists():
            self._dataset_dir.mkdir(parents=True, exist_ok=True)

        self._quantifier_dir = self._path / 'quantifiers'
        if not self._quantifier_dir.exists():
            self._quantifier_dir.mkdir(parents=True, exist_ok=True)

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

    def set_dataset_from_file(self, name, file, overwrite):
        check_name(name)
        fullpath = self._dataset_dir / (name + DATASET_EXTENSION)
        if fullpath.exists() and not overwrite:
            raise FileExistsError(f'A dataset with name {name} already exists.')

        with open(fullpath, 'wb') as outfile:
            shutil.copyfileobj(file.file, outfile)

        df = self.get_dataset(name)
        try:
            label_column_name = get_label_column_name(df)
            text_column_name = get_text_column_name(df)
            if text_column_name is not None:
                description = f'Text dataset, label_column = {label_column_name}, text_column = {text_column_name}'
            else:
                data_column_names = get_data_column_names(df)
                description = f'Numeric dataset, label_column = {label_column_name}, data_columns = [{", ".join(data_column_names)}]'
            self._set_dataset_info(name, 'description', description)
        except:
            self.delete_dataset(name)
            raise
        self._set_dataset_info(name, 'size', len(df))

    def get_dataset(self, name):
        check_name(name)
        fullpath = self._dataset_dir / (name + DATASET_EXTENSION)
        return pd.read_csv(fullpath)

    def delete_dataset(self, name):
        check_name(name)
        fullpath = self._dataset_dir / (name + DATASET_EXTENSION)
        fullpath.unlink(missing_ok=True)
        self.delete_quantifier(name)

    def get_dataset_names(self):
        dataset_names = list()
        for filename in self._dataset_dir.glob('*' + DATASET_EXTENSION):
            dataset_names.append(filename.name[:-len(DATASET_EXTENSION)])
        return dataset_names

    def _set_dataset_info(self, name, field, value):
        check_name(name)
        fullpath = self._dataset_dir / (name + DATASET_INFO_EXTENSION)
        # TODO lock for concurrent updates
        if fullpath.exists():
            with open(fullpath, mode='rb') as inputfile:
                info = dill.load(inputfile)
        else:
            info = dict()
        info[field] = value
        with open(fullpath, mode='wb') as outputfile:
            dill.dump(info, outputfile)

    def get_dataset_info(self, name):
        check_name(name)
        fullpath = self._dataset_dir / (name + DATASET_EXTENSION)
        created = datetime.datetime.fromtimestamp(fullpath.stat().st_ctime).strftime('%Y-%m-%d %H:%M:%S')
        fullpath = self._dataset_dir / (name + DATASET_INFO_EXTENSION)
        with open(fullpath, mode='rb') as inputfile:
            info = dill.load(inputfile)

        return {
            'name': name,
            'created': created,
            'size': info.get('size', 'n/a'),
            'description': info.get('description', 'n/a'),
            'quantifier': info.get('quantifier', 'n/a')
        }

    def get_dataset_count(self):
        return len(list(self._dataset_dir.iterdir()))

    def set_quantifier(self, name, quantifier, overwrite=False):
        check_name(name)
        fullpath = self._quantifier_dir / (name + QUANTIFIER_EXTENSION)
        if fullpath.exists() and not overwrite:
            raise FileExistsError(f'A quantifier with name "{name}" already exists.')

        with open(fullpath, mode='wb') as outputfile:
            dill.dump(quantifier, outputfile)
        self._set_dataset_info(name, 'quantifier', str(quantifier))

    def delete_quantifier(self, name):
        check_name(name)
        fullpath = self._quantifier_dir / (name + QUANTIFIER_EXTENSION)
        fullpath.unlink(missing_ok=True)

    def get_quantifier(self, name):
        fullpath = self._quantifier_dir / (name + QUANTIFIER_EXTENSION)
        if fullpath.exists():
            with open(fullpath, mode='rb') as inputfile:
                return dill.load(inputfile)
        return None

    def get_quantifier_names(self):
        quantifier_names = list()
        for filename in self._quantifier_dir.glob('*' + QUANTIFIER_EXTENSION):
            quantifier_names.append(filename.name[:-len(QUANTIFIER_EXTENSION)])
        return quantifier_names

    def get_quantifier_count(self):
        return len(list(self._quantifier_dir.iterdir()))

    def create_job(self, function, kwargs):
        job_id = f'{datetime_now_to_filename()}.{shortuuid.uuid()}'

        jobfile = self._job_dir / (f'{job_id}.{JobStatus.creating.value}')
        with open(jobfile, mode='wb') as outputfile:
            dill.dump((function, kwargs), outputfile)
        jobfile.rename(self._job_dir / (f'{job_id}.{JobStatus.pending.value}'))

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

    def set_job_done(self, job_id):
        job_filename = self._job_dir / next(self._job_dir.glob(f'{job_id}*'))
        new_filename = self._job_dir / f'{job_filename.name[:job_filename.name.rfind(".")]}.{datetime_now_to_filename()}.{JobStatus.done.value}'
        job_filename.rename(new_filename)

    def set_job_failed(self, job_id):
        job_filename = self._job_dir / next(self._job_dir.glob(f'{job_id}*'))
        new_filename = self._job_dir / f'{job_filename.name[:job_filename.name.rfind(".")]}.{datetime_now_to_filename()}.{JobStatus.error.value}'
        job_filename.rename(new_filename)

    def get_job_ids(self):
        return [job_file.name[:job_file.name.find('.', job_file.name.find('.') + 1)] for job_file in
                self._job_dir.iterdir()]

    def get_job_info(self, job_id):
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
        return {'job_id': job_id, 'function': function.__name__, 'arguments': str(kwargs), 'status': status,
                'created': created, 'started': started, 'completed': completed}

    def get_job_count(self):
        return len(list(self._job_dir.iterdir()))

    def delete_job(self, job_id):
        filename = next(self._job_dir.glob(f'{job_id}*'))
        filename.unlink(missing_ok=True)
        log_file = self._log_dir / f'{job_id}{LOG_EXTENSION}'
        log_file.unlink(missing_ok=True)

    def rerun_job(self, job_id):
        filename = next(self._job_dir.glob(f'{job_id}*'))
        job_filename = filename.name[:filename.name.find('.', filename.name.find('.') + 1)]
        pending_filename = f'{job_filename}.{JobStatus.pending.value}'
        log_file = self._log_dir / f'{job_filename}{LOG_EXTENSION}'
        log_file.unlink(missing_ok=True)
        filename.rename(self._job_dir / pending_filename)

    def get_job_log_stream(self, job_id):
        log_file = self._log_dir / f'{job_id}{LOG_EXTENSION}'
        return open(log_file, mode='wt', encoding='utf-8')

    def get_job_log_content(self, job_id):
        log_file = self._log_dir / f'{job_id}{LOG_EXTENSION}'
        if not log_file.exists():
            return ''
        else:
            with open(log_file, mode='rt', encoding='utf-8') as inputfile:
                return inputfile.read()
