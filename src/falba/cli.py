import argparse
from typing import Any

import celpy


class UserError(Exception):
    pass


def eval_cel_predicate(expr: str, activation: dict[str, Any]) -> bool:
    env = celpy.Environment()
    ast = env.compile(expr)
    prog = env.program(ast)
    try:
        result = prog.evaluate(activation)
    except celpy.CELEvalError as e:  # pyright: ignore
        raise UserError(
            f"CEL evaluation error. Sorry I don't know how to make this error readable:\n{e!s}"
        ) from e
    if not isinstance(result, bool) and not isinstance(result, int):
        raise UserError(
            f"CEL expression returned {result!r}, should return a boolean or an integer"
        )
    return bool(result)


def cmd_ab(args: argparse.Namespace):
    print(eval_cel_predicate(args.expr, {"foo": 1}))


def main() -> int:
    parser = argparse.ArgumentParser(description="Falba CLI")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = True  # Ensures a subcommand must be specified

    ab_parser = subparsers.add_parser("ab", help="Run A/B test")
    ab_parser.add_argument("expr")
    ab_parser.set_defaults(func=cmd_ab)

    args = parser.parse_args()

    try:
        args.func(args)
    except UserError as e:
        print(str(e))
        return 1
    return 0


if __name__ == "__main__":
    main()
