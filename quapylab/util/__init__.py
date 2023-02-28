from pathlib import Path


def get_quapylab_home():
    home = Path.home() / 'quapylab_data'
    home.mkdir(parents=True,exist_ok=True)
    return home