import pathlib

from .model import Db
from .enrichers import ENRICHERS
from .derivers import DERIVERS

def read_db(path: pathlib.Path) -> model.Db:
    """Import a database and run all enrichers and derivers."""
    db = Db.read_dir(path)
    for enricher in ENRICHERS:
        db.enrich_with(enricher)
    for deriver in DERIVERS:
        db.derive_with(deriver)
    return db
