import pathlib

from . import derivers, enrichers, model


def read_db(path: pathlib.Path) -> model.Db:
    """Import a database and run all enrichers and derivers."""
    db = model.Db.read_dir(path)
    for enricher in enrichers.ENRICHERS:
        db.enrich_with(enricher)
    for deriver in derivers.DERIVERS:
        db.derive_with(deriver)
    return db
