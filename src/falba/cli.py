import argparse


def cmd_ab(args):
    print(f"hello world {args.expr}")


def main():
    parser = argparse.ArgumentParser(description="Falba CLI")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = True  # Ensures a subcommand must be specified

    ab_parser = subparsers.add_parser("ab", help="Run A/B test")
    ab_parser.add_argument("expr")
    ab_parser.set_defaults(func=cmd_ab)

    args = parser.parse_args()

    args.func(args)


if __name__ == "__main__":
    main()
