#
# This is an under-designed prototype for a generic data model for benchmark outputs
#

import pandas as pd
import pathlib

from collections.abc import Sequence
from typing import Dict, List, Self, Any, Optional, Callable, Tuple

class _BaseMetric:
  def __init__(self, name: str, value: Any, unit: Optional[str] = None):
    self.name = name
    self.unit = unit
    self.value = value

class Metric(_BaseMetric):
  pass

class Fact(_BaseMetric):
  pass

class Artifact:
    def __init__(self, path: pathlib.Path):
        if not path.exists:
            raise ValueError(f"{path} doesn't exist, can't create artifact")
        self.path = path

    def content(self) -> bytes:
      return self.path.read_bytes()

    def json(self) -> dict:
      with open(self.path, "rb") as f:
        return json.load(f)

class Result:
    def __init__(self, result_dirname: str, artifacts: Dict[pathlib.Path, Artifact], children: List[Self]):
        self.test_name, self.result_id = result_dirname.rsplit(':', maxsplit=1)
        self.result_id = result_dirname
        self.artifacts = artifacts
        self.children = children
        self.facts = {}
        self.metrics = []

    @classmethod
    def read_dir(cls, dire: pathlib.Path) -> Self:
        if not dire.is_dir():
            raise RuntimeError(f"{dire} not a directory, can't be read as a Result")
        return cls(
            result_dirname=dire.name,
            artifacts={p: Artifact(p) for p in dire.glob('artifacts/**/*') if not p.is_dir()},
            children={p.name: Self.read_dir(p) for p in dire.glob('children/*')},
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
    def __init__(self, results: Dict[str, Result]):
        self.results = results

    @classmethod
    def read_dir(cls, dire: pathlib.Path) -> Self:
        return cls(
            results={p.name: Result.read_dir(p) for p in dire.iterdir()}
        )

    def flat_df(self) -> pd.DataFrame:
      rows = []
      for result_id, result in self.results.items():
        for metric in result.metrics:
          row = {"result_id": result_id, "test_name": result.test_name, "metric": metric.name, "value": metric.value, "unit": metric.unit}
          for fact in result.facts.values():
            row[fact.name] = fact.value
          rows.append(row)
      return pd.DataFrame(rows)

    # An enricher extracts metrics and facts from artifacts
    def enrich_with(self, enricher: Callable[[Artifact], Tuple[Sequence[Fact], Sequence[Metric]]]):
      for result in self.results.values():
        for artifact in result.artifacts.values():
          try:
            facts, metrics = enricher(artifact)
          except Exception as e:
            raise RuntimeError(f"failed to enrich artifact: {artifact.path}") from e

          for fact in facts:
            result.add_fact(fact)
          for metric in metrics:
            result.add_metric(metric)

    # A deriver extracts metrics and facts from other metrics and facts
    # TODO: should derivers declare which facts they consume and which they product?
    # TODO: should we record the derivation of facts and metrics in the DB?
    def derive_with(self, deriver: Callable[[Result], Tuple[Sequence[Fact], Sequence[Metric]]]):
      for result in self.results.values():
          facts, metrics = deriver(result)
          for fact in facts:
            result.add_fact(fact)
          for metric in metrics:
            result.add_metric(metric)

# TODO: I wish this design didn't involve so much mutation.