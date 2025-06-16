import argparse
import hashlib
import logging
import math
import os
import pathlib
import shutil
from typing import Any

import polars as pl

import falba


def hist_to_unicode(hist: pl.Series, max_bin_count: int) -> str:
    """Plot a Polars histogram as a line of unicode block elements.

    The width of the plot is implied by the number of rows in the input Series.
    Each row is a bin which are assumed to be of equal size. The max_bin_count
    is the 'y-axis scale', i.e. bins of that size will use the full height of
    the plot."""

    ret = ""
    block_elems = [" ", "▂", "▃", "▄", "▅", "▆", "▇", "█"]
    for bin_count in hist:
        # THIS IS DEFINITELY WRONG I'M PRETTY SURE THIS TYPE OF SHIT WAS EASY
        # WHEN I WAS 12 BUT NOW IT IS HARD
        level = (bin_count / max_bin_count) * (len(block_elems) - 1)
        ret += block_elems[math.floor(level)]
    return ret


def compare(
    db: falba.Db,
    test_name: str | None,
    facts_eq: dict[str, Any],
    ignore_facts: set[str],
    experiment_fact: str,
    metric: str,
):
    df = db.flat_df()

    # TODO: This should be done in Pandas or DuckDB or something, but don't
    # wanna bake in a schema just now.

    # Raise an error if any facts were specified that don't exist for any
    # result.
    extant_facts = db.unique_facts()
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
        if fact == experiment_fact or fact in facts_eq or fact in ignore_facts:
            continue
        vals = set()
        for result in results:
            if fact in result.facts:
                vals.add(result.facts[fact].value)
            else:
                vals.add(None)
        if len(vals) > 1:
            raise RuntimeError(
                f"Multiple values encountered for fact {fact}: {vals}\n"
                + "Try constraining with --fact-eq, or ignoring with --ignore-fact."
            )

    # Lol now I switched to Pandas after all.
    df = (
        db.flat_df()
        .filter(pl.col("result_id").is_in({r.result_id for r in results}))
        .drop(ignore_facts)
    )
    if not len(df):
        raise RuntimeError("No results matched fact predicates")

    # Just get the metrics we're comparing.
    df = df.filter(pl.col("metric") == metric)
    if not len(df):
        avail_metrics = df.select(pl.col("metric").unique()).rows()
        raise RuntimeError(
            f"No results for metric {metric!r}.\n"
            + f"Available metrics for seclected results: {avail_metrics}"
        )

    # Check all the results are for the same test.
    if test_name is not None:
        df = df.filter(pl.col("test_name").eq(test_name))
    test_names = set(df.select("test_name").unique().to_series())
    if len(test_names) != 1:
        raise RuntimeError(
            f"Multiple tests for these metrics: {test_names}. Try constraining with --test"
        )

    # Below we're gonna do dumb shit that makes assumptions about the fact and
    # metric type, so preface that with some dumb rules. Note that also we've
    # already done dumb shit in the rest of Falba which effectively assumes
    # metrics are floats. I dunno this thing kinda sucks and should probably
    # just be written in a proper language, or maybe we need to rearchitect it
    # to less directly expose the user to the untypedness of the Polars API
    # we're using.
    # 1. Assuming we can convert the max value to a float.
    if (dtype := df["value"].dtype) not in [pl.Int64, pl.Float64]:
        raise NotImplementedError(
            f"Command only implemented for numeric metrics ({metric!r} is {dtype})"
        )
    # 2. Assuming we can do maths with the result and not get bullshit.
    df = df.filter(pl.col("value").is_not_null())
    # 3. Assuming we can use the fact value as a dict key.
    if (dtype := df[experiment_fact].dtype) in [pl.List, pl.Array, pl.Object, pl.Struct]:
        raise NotImplementedError(
            f"Command only implemented for scalar facts ({experiment_fact!r} is {dtype})"
        )

    # Determine x-axis scale for histogram plot.
    # TODO: Pick width properly based on terminal and other shit we have to print.
    plot_width = 65
    max_value = float(df["value"].max())  # pyright: ignore
    bin_step = max_value / plot_width
    # The edges are inclusive, we need to include both 0 and the max value in
    # this list. This is why we add 1 to plot_width.
    bin_edges = [(j) * bin_step for j in range(plot_width + 1)]

    # Determine y-axis scale.
    hists = {}
    groups = {}
    for (fact_value,), group in df.group_by(pl.col(experiment_fact)):
        groups[fact_value] = group
        hists[fact_value] = group["value"].hist(bins=bin_edges)
    max_bin_count = max(hist["count"].max() for hist in hists.values())

    # Print stuff. Do this using a stable ordering.
    keys = sorted(hists.keys())

    for fact_value in sorted(hists.keys()):
        hist = hists[fact_value]
        group = groups[fact_value]

        print("\n")
        # Hack to print numbers and stuff with a readable alignment: throw them
        # into a DataFrame.
        print(
            pl.DataFrame(
                [
                    {
                        "samples": len(group),
                        "mean": group["value"].mean(),
                        "max": group["value"].max(),
                        "min": group["value"].min(),
                        experiment_fact: fact_value,
                    }
                ]
            )
        )
        hist_plot = hist_to_unicode(hist["count"], max_bin_count)
        print(hist_plot)
        print(f"|{'-' * (len(hist_plot) - 2)}|")

    # Show "graph X-axis"
    print(f"0{max_value:>65}")


def import_result(db: falba.Db, test_name: str, artifact_paths: list[pathlib.Path]):
    """Add a result to the database. Update the db in memory too.

    Files specified directly are added by name to the root of the artifacts
    tree. Directories are copied recursively, preserving the their structure.
    """

    # Helper to walk through the files in a way that reflects the structure of
    # the artifacts directory at the end.
    # Yields tuiples of (current path of file, eventual path of file relative to
    # artifacts/)
    def iter_artifacts():
        for input_path in artifact_paths:
            if input_path.is_dir():
                for dirpath, _, filenames in input_path.walk():
                    for filename in filenames:
                        cur_path = dirpath / filename
                        yield cur_path, cur_path.relative_to(input_path)
            else:
                yield input_path, input_path.name

    # Figure out the result ID by hashing the artifacts.
    hash = hashlib.sha256()
    for path, _ in iter_artifacts():
        # Doesn't seem to be a concise way to update a hash object
        # from a file, you can only get a digest for the whole file
        # at once so just do that and then we'll hash the hashes.
        with open(path, "rb") as f:
            hash.update(hashlib.file_digest(f, "sha256").digest())

    # Copy the artifacts into the database.
    result_dir = db.root_dir / f"{test_name}:{hash.hexdigest()[:12]}"
    # This must fail if the directory already exists.
    os.mkdir(result_dir)
    artifacts_dir = result_dir / "artifacts"
    num_copied = 0
    for cur_path, artifact_relpath in iter_artifacts():
        artifact_path = artifacts_dir / artifact_relpath
        # Since we know artifacts_dir is new, we don't care if this fails. This
        # means if the user provides duplicate inputs, meh.
        os.makedirs(artifact_path.parent, exist_ok=True)
        shutil.copy(cur_path, artifact_path)
        num_copied += 1

    logging.info(f"Imported {num_copied} artifacts to {result_dir}")


def ls_results(db: falba.Db):
    print(db.results_df())


def ls_metrics(db: falba.Db):
    print(db.flat_df())


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    # Print entire DataFrames/Series instead of truncating.
    pl.Config.set_tbl_rows(-1)
    # Make prints of DataFrame a bit more concise.
    pl.Config.set_tbl_hide_dataframe_shape(True)
    pl.Config.set_tbl_hide_column_data_types(True)

    parser = argparse.ArgumentParser(description="Falba CLI")
    parser.add_argument("--result-db", default="./results", type=pathlib.Path)

    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = True

    def cmd_compare(args: argparse.Namespace):
        facts_eq = {name: val for [name, val] in args.fact_eq}
        for [name, s] in args.fact_eq_bool:
            str_to_bool = {
                "true": True,
                "false": False,
                "none": None,  # lmao
            }
            if s not in str_to_bool:
                raise argparse.ArgumentTypeError("Bool must be 'true', 'false' or 'none' lmao")
            facts_eq[name] = str_to_bool[s]
        compare(
            db=db,
            test_name=args.test,
            facts_eq=facts_eq,
            ignore_facts=set(args.ignore_fact),
            experiment_fact=args.experiment_fact,
            metric=args.metric,
        )

    compare_parser = subparsers.add_parser("compare", help="Run A/B test")
    compare_parser.add_argument("experiment_fact")
    compare_parser.add_argument("metric")
    compare_parser.add_argument("--test", help="Test name to compare results for")
    compare_parser.add_argument(
        "--fact-eq",
        action="append",
        default=[],
        nargs=2,
        metavar=("fact", "value"),
        help=(
            "Specify a fact and its value (e.g., --fact-eq fact1 val1) "
            + "Comparison will be filtered to only include results matching this equality."
        ),
    )
    compare_parser.add_argument(
        "--fact-eq-bool",
        action="append",
        default=[],
        nargs=2,
        metavar=("fact", "value"),
        help=(
            "Specify a fact and its value (e.g., --fact-eq-bool fact1 true) "
            + "Comparison will be filtered to only include results matching this equality."
        ),
    )
    compare_parser.add_argument(
        "--ignore-fact",
        action="append",
        default=[],
        metavar="fact",
        help="Specify a fact to ignore",
    )
    compare_parser.set_defaults(func=cmd_compare)

    def cmd_import(args: argparse.Namespace):
        import_result(db, args.test_name, args.file)

    import_parser = subparsers.add_parser("import", help="Import a result to the database")

    def valid_test_name(s: str) -> str:
        # Dumb hack to avoid dealing with the fact that they are used to
        # construct filenames, just forbid path separators. Also exclude MS-DOS
        # separators in case anyone is benchmarking their retrocomputing
        # projects.
        if "/" in s or "\\" in s:
            raise argparse.ArgumentTypeError(f"Test names must not contain '/' or '\\' ({s!r})")
        return s

    import_parser.add_argument("test_name", type=valid_test_name)
    import_parser.add_argument("file", nargs="+", type=pathlib.Path)
    import_parser.set_defaults(func=cmd_import)

    def cmd_ls_results(args: argparse.Namespace):
        ls_results(db)

    ls_parser = subparsers.add_parser("ls-results", help="List results in the database")
    ls_parser.set_defaults(func=cmd_ls_results)

    def cmd_ls_metrics(args: argparse.Namespace):
        ls_metrics(db)

    ls_parser = subparsers.add_parser("ls-metrics", help="List metrics in the database")
    ls_parser.set_defaults(func=cmd_ls_metrics)

    args = parser.parse_args()

    db = falba.read_db(args.result_db)

    args.func(args)


if __name__ == "__main__":
    main()
