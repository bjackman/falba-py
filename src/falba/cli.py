import argparse
import pathlib
from typing import Any

import celpy
import lark

import falba


class UserError(Exception):
    pass


def eval_cel_pred(prog: celpy.Runner, activation: dict[str, Any]) -> bool:
    try:
        result = prog.evaluate(activation)
    except celpy.CELEvalError as e:  # pyright: ignore
        raise UserError(
            f"CEL evaluation error. Sorry I don't know how to make this error readable:\n{e!s}\nActivation was:{activation!r}"
        ) from e
    if not isinstance(result, bool) and not isinstance(result, int):
        raise UserError(
            f"CEL expression returned {result!r}, should return a boolean or an integer"
        )
    return bool(result)


def do_ab(db: falba.Db, expr: str):
    env = celpy.Environment()
    ast = env.compile(expr)

    # Try to figure out what identifiers are referenced in the CEL program.
    # This is using an API that is not really documented and is probably
    # completely wrong. What is a "value"? No fucking idea.
    referenced_idents = []
    for value in ast.scan_values(lambda v: True):
        if isinstance(value, lark.Token) and value.type == "IDENT":
            referenced_idents.append(value.value)

    prog = env.program(ast)

    results = [r for r in db.results.values() if eval_cel_pred(prog, r.fact_vals())]


def cmd_ab(db: falba.Db, args: argparse.Namespace):
    print(do_ab(db, args.expr))


def main() -> int:
    parser = argparse.ArgumentParser(description="Falba CLI")
    parser.add_argument("--result-db", default="./results", type=pathlib.Path)

    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = True

    ab_parser = subparsers.add_parser("ab", help="Run A/B test")
    ab_parser.add_argument("expr")
    ab_parser.set_defaults(func=cmd_ab)

    args = parser.parse_args()

    db = falba.read_db(args.result_db)

    try:
        args.func(db, args)
    except UserError as e:
        print(str(e))
        return 1
    return 0


if __name__ == "__main__":
    main()
