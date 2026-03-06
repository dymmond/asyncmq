# Contributing

## Ways to Contribute

- report bugs and regressions
- improve tests and docs
- propose focused feature additions
- review pull requests

Issue tracker: [github.com/dymmond/asyncmq/issues](https://github.com/dymmond/asyncmq/issues)

## Development Setup

```bash
git clone https://github.com/dymmond/asyncmq
cd asyncmq
pip install hatch
hatch env create
```

## Run Tests

```bash
hatch run test:test
```

Run a subset:

```bash
hatch run test:test tests/cli -q
```

## Lint and Types

```bash
hatch run lint
hatch run check_types
```

## Docs

```bash
hatch run docs:prepare
hatch run docs:build
hatch run docs:serve
```

## Pull Request Guidance

- keep changes focused and justified
- add or update tests for behavior changes
- update docs for user-facing changes
- avoid unrelated refactors in the same PR
