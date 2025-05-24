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
- Add pylint/pyfmt/type checker/etc
- Add some tests