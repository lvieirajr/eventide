# Contributing to Eventide

First of all, thank you for your interest in contributing!
Eventide is a small, focused project and contributions are very welcome.

---

## How to Contribute

- **Bug Reports** — Open an issue if you find unexpected behavior.
- **Feature Requests** — Open an issue describing the requested feature.
- **Pull Requests** — Submit code fixes, improvements, or documentation updates.

---

## Getting Started

To contribute to Eventide, you will need:

- `make`
- [mise](https://mise.jdx.dev/) (tool/version manager)

Everything else will be managed by mise in a project environment.


---

## Setup Instructions

1. Clone the repository:

```bash
git clone https://github.com/lvieirajr/eventide.git
cd eventide
```

2. Bootstrap your local environment using the Makefile:

```bash
make bootstrap
```

This will:
- Install all required tools via `mise`
- Install all Python dependencies via `uv`
- Set up `pre-commit` hooks

---

## Development Workflow

- To run the test suite:

```bash
make pytest
```

- To run linting and formatting:

```bash
make ruff
```

- To run static type checking:

```bash
make mypy
```

- To update or reinstall all dependencies:

```bash
make sync
```

- To view available commands:

```bash
make help
```

---

## Pull Request Guidelines

- Keep pull requests small, focused, and clear.
- Make sure `make pytest`, `make ruff`, and `make mypy` pass.
- Update or add tests when introducing new features or fixing bugs.
- Match the project's style and structure.
- Document any public-facing changes clearly.

---

## Code Style

- Code must pass [ruff](https://docs.astral.sh/ruff/) formatting and linting.
- Static types are enforced with [mypy](http://mypy-lang.org/).
- [pre-commit](https://pre-commit.com/) hooks must pass.

---

## Thank You

Every contribution, no matter how small, helps improve Eventide.
Thank you for your time and effort!
