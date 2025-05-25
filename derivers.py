import logging
from collections.abc import Sequence
from typing import Dict, List, Self, Any, Optional, Callable, Tuple

from . import model


def derive_asi_on(
    result: model.Result,
) -> Tuple[Sequence[model.Fact], Sequence[model.Metric]]:
    try:
        kconfig = result.facts["kconfig"].value
        cmdline_fields = result.facts["cmdline_fields"].value
    except KeyError as e:
        logging.debug(
            f"{result.result_id}: Couldn't derive ASI enablement: missing {e}"
        )
        return [], []

    asi_builtin = kconfig.get("CONFIG_MITIGATION_ADDRESS_SPACE_ISOLATION") == "y"
    if asi_builtin:
        asi_default_on = kconfig.get("CONFIG_ADDRESS_SPACE_ISOLATION_DEFAULT_ON") == "y"
        if asi_default_on:
            asi_on = cmdline_fields.get("asi") != "off"
        else:
            asi_on = cmdline_fields.get("asi") == "on"
        if asi_on:
            if cmdline_fields.get("asi_userspace") == "off":
                fact_val = "kvm_only"
            else:
                fact_val = "yes"
        else:
            fact_val = "no"
    else:
        fact_val = "no"

    return ([model.Fact(name="asi_on", value=fact_val)], [])


def derive_retbleed_mitigation(
    result: model.Result,
) -> Tuple[Sequence[model.Fact], Sequence[model.Metric]]:
    try:
        asi_on = result.facts["asi_on"].value
        sysfs_mit = result.facts["sysfs_cpu_vuln:retbleed"].value
    except KeyError:
        logging.debug(
            f"{result.result_id}: couldn't derive retbleed mitigation, facts:, {result.facts.keys()}"
        )
        return [], []

    if not asi_on:
        mit = sysfs_mit
    elif sysfs_mit != "Vulnerable":
        mit = f"ASI + {sysfs_mit}"
    else:
        mit = "ASI"

    return ([model.Fact(name="retbleed_mitigation", value=mit)], [])


# Hack to implement a fact with a "default value" of False.
def derive_default_instrumented(
    result: model.Result,
) -> Tuple[Sequence[model.Fact], Sequence[model.Metric]]:
    if result.facts.get("instrumented"):
        return ([], [])
    return ([model.Fact(name="instrumented", value=False)], [])


DERIVERS = [derive_asi_on, derive_retbleed_mitigation, derive_default_instrumented]
