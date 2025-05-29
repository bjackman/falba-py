import pathlib

from . import enrichers, model
from .model import Db, Result


def read_db(path: pathlib.Path) -> model.Db:
    """Import a database and run all enrichers"""
    return model.Db.read_dir(path, enrichers.ENRICHERS)
