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
    def read_dir(cls, dire: pathlib.Path) -> Self:
        if not dire.is_dir():
            raise RuntimeError(f"{dire} not a directory, can't be read as a Result")
        return cls(
            result_dirname=dire.name,
            artifacts={
                p: Artifact(p) for p in dire.glob("artifacts/**/*") if not p.is_dir()
            },
        )

    def add_fact(self, fact: Fact):
        """Add a fact about the system or the test.

        Only one fact with a given name is allowed.
        """
        if fact.name in self.facts:
            raise ValueError(f"fact {fact.name} already exists")

        self.facts[fact.name] = fact

    def add_metric(self, metric: Metric):
        """Add a metric, which is the thing the test was measuring.

        Multiple samples of the same metric are allowed."""
        self.metrics.append(metric)


class Db:
    def __init__(self, results: dict[str, Result]):
        self.results = results

    @classmethod
    def read_dir(cls, dire: pathlib.Path) -> Self:
        return cls(results={p.name: Result.read_dir(p) for p in dire.iterdir()})

    def flat_df(self) -> pl.DataFrame:
        rows = []
        for result_id, result in self.results.items():
            for metric in result.metrics:
                row = {
                    "result_id": result_id,
                    "test_name": result.test_name,
                    "metric": metric.name,
                    "value": metric.value,
                    "unit": metric.unit,
                }
                for fact in result.facts.values():
                    row[fact.name] = fact.value
                rows.append(row)
        return pl.DataFrame(rows)

    # An enricher extracts metrics and facts from artifacts
    def enrich_with(
        self, enricher: Callable[[Artifact], tuple[Sequence[Fact], Sequence[Metric]]]
    ):
        for result in self.results.values():
            for artifact in result.artifacts.values():
                try:
                    facts, metrics = enricher(artifact)
                except Exception as e:
                    raise RuntimeError(
                        f"failed to enrich artifact: {artifact.path}"
                    ) from e

                for fact in facts:
                    result.add_fact(fact)
                for metric in metrics:
                    result.add_metric(metric)


# TODO: I wish this design didn't involve so much mutation.
