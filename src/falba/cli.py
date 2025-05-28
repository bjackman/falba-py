import argparse
import pathlib

import falba


def cmd_ab(args: argparse.Namespace):
    print(f"hello world {args.expr}")


def main():
    parser = argparse.ArgumentParser(description="Falba CLI")
    parser.add_argument("--result-db", default="./results", type=pathlib.Path)

    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = True

    ab_parser = subparsers.add_parser("ab", help="Run A/B test")
    ab_parser.add_argument("expr")
    ab_parser.set_defaults(func=cmd_ab)

    args = parser.parse_args()

    db = falba.read_db(args.result_db)

    args.func(args)


if __name__ == "__main__":
    main()
