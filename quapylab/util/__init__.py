import datetime
from pathlib import Path


def get_quapylab_home():
    home = Path.home() / 'quapylab_data'
    home.mkdir(parents=True,exist_ok=True)
    return home

def datetime_now_to_filename():
    job_name = str(datetime.datetime.now())
    job_name = job_name[:job_name.rfind(".")]
    job_name = job_name.replace(' ', '_')
    return job_name.replace(':', '-')