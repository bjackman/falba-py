import pathlib

from . import enrichers, model
from .model import Db, Result


def read_db(path: pathlib.Path) -> model.Db:
    """Import a database and run all enrichers"""
    db = model.Db.read_dir(path)
    for enricher in enrichers.ENRICHERS:
        db.enrich_with(enricher)
    return db
