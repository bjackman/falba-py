#
# This is an under-designed prototype for a generic data model for benchmark outputs
#

import json
import pathlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Generic, Self, TypeVar

import polars as pl

T = TypeVar("T")


@dataclass(frozen=True)
class _BaseMetric(Generic[T]):
    name: str
    value: T
    unit: str | None = None


class Metric(_BaseMetric[T]):
    pass


class Fact(_BaseMetric[T]):
    pass


@dataclass
class Artifact:
    path: pathlib.Path

    def __post_init__(self):
        if not self.path.exists:
            raise ValueError(f"{self.path} doesn't exist, can't create artifact")

    def content(self) -> bytes:
        return self.path.read_bytes()

    def json(self) -> dict:
        with open(self.path, "rb") as f:
            return json.load(f)


Enricher = Callable[[Artifact], tuple[Sequence[Fact], Sequence[Metric]]]


@dataclass
class Result:
    result_dirname: str
    artifacts: dict[pathlib.Path, Artifact]
    test_name: str = field(init=False)
    result_id: str = field(init=False)
    facts: dict[str, Fact] = field(default_factory=dict)
    metrics: list[Metric] = field(default_factory=list)

    def __post_init__(self):
        self.test_name, self.result_id = self.result_dirname.rsplit(":", maxsplit=1)

    @classmethod
    def read_dir(cls, dire: pathlib.Path, enrichers: list[Enricher]) -> Self:
        if not dire.is_dir():
            raise RuntimeError(f"{dire} not a directory, can't be read as a Result")
        artifacts = {p: Artifact(p) for p in dire.glob("artifacts/**/*") if not p.is_dir()}

        # Call all enrichers, checking for forbidden duplicate attributes.
        fact_to_enricher = {}
        facts = {}
        metrics = []
        for enricher in enrichers:
            for artifact in artifacts.values():
                new_facts, new_metrics = enricher(artifact)
                for fact in new_facts:
                    if other_enricher := fact_to_enricher.get(fact.name):
                        raise RuntimeError(
                            f"Enricher {enricher.__name__} produced fact {fact!r} "
                            + f"but this was already produced by enricher {other_enricher.__name__}"
                        )
                    facts[fact.name] = fact
                    fact_to_enricher[fact.name] = enricher
                for metric in new_metrics:
                    if other_enricher := fact_to_enricher.get(metric.name):
                        raise RuntimeError(
                            f"Enricher {enricher.__name__} produced metric {metric!r} "
                            + f"but a fact by this name was already produced by enricher "
                            + other_enricher.__name__
                        )
                    metrics.append(metric)

        return cls(
            result_dirname=dire.name,
            artifacts=artifacts,
            facts=facts,
            metrics=metrics,
        )


@dataclass
class Db:
    results: dict[str, Result]
    root_dir: pathlib.Path

    @classmethod
    def read_dir(cls, dire: pathlib.Path, enrichers: list[Enricher]) -> Self:
        results = {}
        for p in dire.iterdir():
            if dire.name == "results.json":
                continue  # falba-go configuration
            results[p.name] = Result.read_dir(p, enrichers)
        return cls(
            results=results,
            root_dir=dire,
        )

    def unique_facts(self) -> set[str]:
        """Return all fact names in the DB."""
        facts = set()
        for result in self.results.values():
            facts |= result.facts.keys()
        return facts

    def results_df(self) -> pl.DataFrame:
        """Return a DataFrame with a row for each result."""
        rows = []
        for result in self.results.values():
            row = {
                "result_id": result.result_id,
                "test_name": result.test_name,
            }
            for fact in result.facts.values():
                row[fact.name] = fact.value
            rows.append(row)
        return pl.DataFrame(rows)

    def flat_df(self) -> pl.DataFrame:
        """Return a DataFrame with a row for each metric."""
        rows = []
        for result in self.results.values():
            for metric in result.metrics:
                row = {
                    "result_id": result.result_id,
                    "test_name": result.test_name,
                    "metric": metric.name,
                    "value": metric.value,
                    "unit": metric.unit or "",
                }
                for fact in result.facts.values():
                    row[fact.name] = fact.value
                rows.append(row)
        schema = ["result_id", "test_name", "metric", "value", "unit"] + sorted(self.unique_facts())
        return pl.DataFrame(rows, schema=schema, infer_schema_length=None)
