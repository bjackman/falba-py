typecheck:
  uv run pyright

format:
  uv run ruff format

lint:
  uv run ruff check

fix: format
  uv run ruff check --fix

fix-unsafe: format
  uv run ruff check --fix --unsafe-fixes

test:
  uv run pytest

check: typecheck format lint test

default: check