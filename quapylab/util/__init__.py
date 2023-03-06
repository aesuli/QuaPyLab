import datetime
from pathlib import Path


def get_quapylab_home():
    home = Path.home() / 'quapylab_data'
    home.mkdir(parents=True,exist_ok=True)
    return home

def datetime_now_to_filename():
    job_id = str(datetime.datetime.now())
    job_id = job_id[:job_id.rfind(".")]
    job_id = job_id.replace(' ', '_')
    return job_id.replace(':', '-')