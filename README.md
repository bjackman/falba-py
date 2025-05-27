# FALBA

This is an experimental library for managing and analysing benchmarking results.
It's vendored into my ASI benchmarking repository while I prototype it, it
doesn't really belong here.

TODO:

- Decide on the data model:
  - What about units?
  - Should facts nest?
  - How should we make facts "type safe" but also flexible?
  - How can we avoid some sort of silly implicit assumption that "facts" all
    describe a single "entity"?
  - The "instrumented" fact has a "default value", I guess this is useful...
- Add some tests
- Decide a better model for implementing/configuring "enrichers"/"derivers" (but
  also maybe just completely rethink the model for ingesting data).

Boring shit I don't care about:

- I put my `testdata` into my `src/` tree. I guess this is somehow wrong. I
  asked an AI how I should do it and it told me to do this incredibly stupid
  "fixture" thing with pytest that I don't want to do.

## Checks

Install `uv` (try `pipx install uv`). Then

- Type check with `uv run pyright`
- Format wuth `uv run ruff format`
- Lint with `uv run ruff check`. Add `--fix` and potentially `--unsafe-fixes` to
  auto fix.
- Test with `uv run pytest`