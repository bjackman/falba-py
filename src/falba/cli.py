import argparse
import pathlib
from typing import Any

import falba


def compare(db: falba.Db, facts_eq: dict[str, Any], experiment_fact: str, metric: str):
    df = db.flat_df()

    # TODO: This should be done in Pandas or DuckDB or something, but don't
    # wanna bake in a schema just now.

    # Raise an error if any facts were specified that don't exist for any
    # result.
    extant_facts = set()
    for result in db.results.values():
        extant_facts |= result.facts.keys()
    missing_facts = set(facts_eq.keys()) - extant_facts
    if missing_facts:
        raise RuntimeError(
            f"Facts {missing_facts} not in any result in DB. Typo? "
            + f"Available facts: {list(extant_facts)}"
        )

    # Filter results based on facts_eq.
    def include_result(result: falba.Result) -> bool:
        for name, required_val in facts_eq.items():
            if name in result.facts and result.facts[name].value != required_val:
                return False
        return True

    results = [r for r in db.results.values() if include_result(r)]

    # Check all facts are either part of the experiment, or equal for all
    # results.
    for fact in extant_facts:
        if fact == experiment_fact or fact in facts_eq:
            continue
        vals = set()
        for result in results:
            if fact in result.facts:
                vals.add(result.facts[fact].value)
            else:
                vals.add(None)
        if len(vals) != 1:
            raise RuntimeError(f"Multiple values encountered for fact {fact}: {vals}")

    # Lol now I switched to Pandas after all.
    df = db.flat_df()
    df = df[df["result_id"].isin(list({r.result_id for r in results}))]
    df = df[df["metric"] == metric]

    for name, group in df.groupby(experiment_fact):  # pyright: ignore
        gv = group["value"]
        print(f"{name:<30} mean {metric}: {gv.mean():>13.1f}")
        print(
            f"  ({', '.join(group['result_id'].unique())}) {len(group):>8} samples  stddev {gv.std():>10.1f}"  # pyright: ignore
        )


def main():
    parser = argparse.ArgumentParser(description="Falba CLI")
    parser.add_argument("--result-db", default="./results", type=pathlib.Path)

    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = True

    compare_parser = subparsers.add_parser("compare", help="Run A/B test")
    compare_parser.add_argument("experiment_fact")
    compare_parser.add_argument("metric")
    compare_parser.add_argument(
        "--fact-eq",
        action="append",
        nargs=2,
        metavar=("fact", "value"),
        help=(
            "Specify a fact and its value (e.g., --fact-eq fact1 val1) "
            + "Comparison will be filtered to only include results matching this equality."
        ),
    )

    def cmd_ab(args: argparse.Namespace):
        compare(
            db,
            {name: val for [name, val] in args.fact_eq},
            args.experiment_fact,
            args.metric,
        )

    compare_parser.set_defaults(func=cmd_ab)

    args = parser.parse_args()

    db = falba.read_db(args.result_db)

    args.func(args)


if __name__ == "__main__":
    main()
