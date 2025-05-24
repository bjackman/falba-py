from collections.abc import Sequence
from typing import Dict, List, Self, Any, Optional, Callable, Tuple
import json
from fnmatch import fnmatch
import tarfile
import os
import re
import shlex
import datetime
import logging
import logging

from . import model

#
# Here are some super hacky examples of things that might become fact/metric extraction plugins
#

class EnrichmentFailure(Exception):
  pass

# Enrichers return (facts, metrics) pairs.

def enrich_from_ansible(artifact: model.Artifact) -> Tuple[Sequence[model.Fact], Sequence[model.Metric]]:
  if artifact.path.name != "ansible_facts.json":
    return [], []
  try:
    ansible_facts = json.loads(artifact.content())
  except json.decoder.JSONDecodeError as e:
    raise EnrichmentFailure() from e

  facts = []
  try:
    # Ansible doesn't give us the raw commandline
    facts.append(model.Metric(name="cmdline_fields", value=ansible_facts["ansible_cmdline"]))
    facts.append(model.Metric(name="nproc", value=ansible_facts["ansible_processor_nproc"]))
    # TODO: would prefer to express this in a way that captures units.
    facts.append(model.Metric(name="memory", value=ansible_facts["ansible_memtotal_mb"], unit="MB"))
    ansible_ansible_facts = ansible_facts["ansible_facts"] # wat
    facts.append(model.Metric(name="kernel_version", value=ansible_ansible_facts["kernel"]))

    ts = ansible_facts["ansible_date_time"]["iso8601_micro"]
    facts.append(model.Metric(name="timestamp", value=datetime.datetime.fromisoformat(ts)))

    # ansible_processor seems to be a list where each consecutive 3 pairs is
    # (processor number, vendor, model)
    ansible_processor = ansible_facts["ansible_processor"]
  except KeyError as e:
    raise EnrichmentFailure("missing field in ansible all_facts") from e

  try:
    p = ansible_processor
    cpu_models = {int(p[i]): (p[i+1] + " " + p[i+2]) for i in range(0, len(p), 3)}
    facts.append(model.Metric(name="cpu", value=" + ".join(set(cpu_models.values()))))

  except Exception as e:
    raise EnrichmentFailure("failed to parse ansible_processor mess") from e

  # TODO: Need to figure out how to encode my knowledge about whose cmdline this is and ideally where it came from.
  #       Probably when I write the results to the database I should be dropping some metadata
  #       saying that this is an ansible fact dump and how it relates to the SUT.
  return (facts, [])

def enrich_from_phoronix_json(artifact: model.Artifact) -> Tuple[Sequence[model.Fact], Sequence[model.Metric]]:
  if not fnmatch(str(artifact.path), "**/pts-results.json"):
    return list[model.Fact](), list[model.Metric]()
  try:
    obj = json.loads(artifact.content())
  except json.decoder.JSONDecodeError as e:
    raise EnrichmentFailure() from e
  metrics: List[model.Metric] = []

  try:
    # In the current data I"m looking at, the key here isa timestamp with no timezone
    for result in obj["results"].values():
      if result["identifier"] != "pts/fio-2.1.0":
        print(f"Ignoring unknown Phoronix result with identifier: {result['identifier']}")
        continue
      # TODO: do we want some general capability for hierarchical results? For now
      # we'll just store metrics directly as items in the result and then flatten
      # this later into a DF or whatever that's easy to do analysis on.
      for subresult in result["results"].values():
        for value in subresult["raw_values"]:
          args = result["arguments"]
          scale = result["scale"]
          metrics.append(model.Metric(name=f"PTS FIO [{args}] {scale}",
                                value=value,
                                unit=result["scale"]))
  except KeyError as e:
    raise EnrichmentFailure("missing expected field in phoronix-test-suite-result.json") from e
  return list[model.Fact](), metrics

def enrich_from_sysfs_tgz(artifact: model.Artifact) -> Tuple[Sequence[model.Fact], Sequence[model.Metric]]:
  if not fnmatch(str(artifact.path), "*/tmp/sysfs_cpu.tgz"):
    return list[model.Fact](), list[model.Metric]()
  try:
    facts = []
    with tarfile.open(artifact.path, 'r:gz') as tar:
            extracted_file = tar.extractfile(member)
            if not fnmatch(member.name, "/sys/devices/system/cpu/vulnerabilities/*"):
              continue
            content = tar.extractfile(member).read().decode('utf-8')
            # tar is too clever and gets confused by sysfs files, strip of the NULs it adds
                facts.append(model.Fact(name=f"sysfs_cpu_vuln:{os.path.basename(member.name)}", value=content.strip('\0').strip())
    return facts, list[model.Metric]()
  except Exception as e:
    raise EnrichmentFailure() from e

# TODO: Should each kconfig actually be a separate fact? Maybe facts shoudl inherently be nesting...
def enrich_from_kconfig(artifact: model.Artifact) -> Tuple[Sequence[model.Fact], Sequence[model.Metric]]:
  if not fnmatch(str(artifact.path), "*/kconfig"):
    return list[model.Fact](), list[model.Metric]()
  kconfig_dict = {}
  for line in artifact.content().decode().splitlines():
    if not line.strip() or line.startswith("#"):
      continue
    try:
      k, v = line.split("=", maxsplit=1)
      kconfig_dict[k] = v
    except Exception as e:
      raise EnrichmentFailure(f"failed to parse kconfig line: {line}") from e
  return [model.Metric(name="kconfig", value=kconfig_dict)], []

# Reads an /etc/os_release file. Does this selectively...
def enrich_from_os_release(artifact: model.Artifact) -> Tuple[Sequence[model.Fact], Sequence[model.Metric]]:
  if not fnmatch(str(artifact.path), "*/etc_os-release"):
    return list[model.Fact](), list[model.Metric]()

  fields = {}
  for line in artifact.content().decode().splitlines():
    if not line.strip() or line.startswith("#"):
      continue
    k, v = line.split("=", maxsplit=1)
    parts = shlex.split(v)
    if len(parts) != 1:
      raise EnrichmentFailure(f"Seems like an invalid /etc/os-release line (shlex found: {parts}): line")
    fields[k] = parts[0]

  facts, metrics = [], []
  if "VARIANT_ID" in fields:
    facts.append(model.Fact(name="os_release_variant_id", value=fields["VARIANT_ID"]))

  return facts, metrics

# TODO: make the JSON-reading enrichers less boilerplatey

# Reads selected metrics from the output of the FIO benchmark with --output-format=json+
# (Maybe also without the plus, not sure).
def enrich_from_fio_json_plus(artifact: model.Artifact) -> Tuple[Sequence[model.Fact], Sequence[model.Metric]]:
  if not fnmatch(str(artifact.path), "*/fio_output_*.json"):
    return list[model.Fact](), list[model.Metric]()

  try:
    output_obj = json.loads(artifact.content())
  except json.decoder.JSONDecodeError as e:
    raise EnrichmentFailure() from e

  facts, metrics = [], []

  try:
    for job in output_obj["jobs"]:
      for fio_metric in ["lat_ns", "slat_ns", "clat_ns"]:
        metrics.append(model.Metric(name=f"fio_{job["jobname"]}_read_{fio_metric}_mean",
                                    value=job["read"]["clat_ns"]["mean"]))
      metrics.append(model.Metric(name=f"fio_{job["jobname"]}_read_iops",
                                  value=job["read"]["iops"]))
  except KeyError as e:
    raise EnrichmentFailure("missing field in FIO output JSON") from e

  return facts, metrics

# Reads output of `nixos-version --json`
def enrich_from_nixos_version_json(artifact: model.Artifact) -> Tuple[Sequence[model.Fact], Sequence[model.Metric]]:
  if not fnmatch(str(artifact.path), "*/nixos-version.json"):
    return list[model.Fact](), list[model.Metric]()

  try:
    obj = json.loads(artifact.content())
  except json.decoder.JSONDecodeError as e:
    raise EnrichmentFailure() from e

  facts, metrics = [], []

  try:
    facts.append(model.Fact(name="nixos_configuration_revision", value=obj["configurationRevision"]))
  except KeyError as e:
    raise EnrichmentFailure("missing field in FIO output JSON") from e

  return facts, metrics

# Parses results of bpftrace progrogs included in my benchmarking repo.
def enrich_from_bpftrace_logs(artifact: model.Artifact) -> Tuple[Sequence[model.Fact], Sequence[model.Metric]]:
  if not fnmatch(str(artifact.path), "*/bpftrace_asi_exits.log"):
    return list[model.Fact](), list[model.Metric]()

  facts, metrics = [], []

  exits_metric = None
  pattern = r"@total_exits:\s+(\d+)"
  for line in artifact.content().decode().splitlines():
    match = re.search(pattern, line)
    if match:
      if exits_metric:
        logging.warn(f"Found two @total_exits results in {artifact.path}")
      exits_metric = model.Metric(name="asi_exits", value=int(match.group(1)))
  if exits_metric:
    metrics.append(exits_metric)
    facts.append(model.Fact(name="instrumented", value=True))

  return facts, metrics

ENRICHERS = [
    enrich_from_ansible, enrich_from_phoronix_json, enrich_from_sysfs_tgz,
    enrich_from_kconfig, enrich_from_os_release, enrich_from_fio_json_plus,
    enrich_from_nixos_version_json, enrich_from_bpftrace_logs,
]
