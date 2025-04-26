SHELL := /usr/bin/env bash -eou pipefail
.DEFAULT_GOAL := bootstrap


# ======================================================================================
# Bootstrap
# ======================================================================================
.bootstrapped-mise:
	@$(MAKE) mise-setup
	@touch .bootstrapped-mise

.bootstrapped-uv:
	@$(MAKE) sync
	@touch .bootstrapped-uv

.bootstrapped-pre-commit:
	@$(MAKE) pre-commit-install
	@touch .bootstrapped-pre-commit

.PHONY: bootstrap
bootstrap: .bootstrapped-mise .bootstrapped-uv .bootstrapped-pre-commit  ##@Bootstrap Bootstraps the project


# ======================================================================================
# Mise en place
# ======================================================================================
.PHONY: mise-setup
mise-setup: mise.toml  ##@Mise Sets up mise-en-place
	@echo "Setting up mise..."
	@mise trust --yes --quiet --silent
	@mise install --yes --quiet --silent


# ======================================================================================
# UV
# ======================================================================================
.PHONY: lock
lock: pyproject.toml ##@UV Locks the Python dependencies
	@echo "Locking Python dependencies..."
	@uv lock --upgrade

.PHONY: sync
sync: pyproject.toml uv.lock ##@UV Installs the Python dependencies
	@echo "Installing Python dependencies..."
	@uv sync --all-extras --frozen

.PHONY: build
build: pyproject.toml uv.lock ##@UV Builds the package
	@echo "Building the package..."
	@uv build

.PHONY: publish
publish: pyproject.toml uv.lock ##@UV Publishes the package to PyPI
	@echo "Publishing the package to PyPI..."
	@uv publish

.PHONY: publish-test
publish-test: pyproject.toml uv.lock ##@UV Publishes the package to test PyPI
	@echo "Publishing the package to test PyPI..."
	@uv publish --index testpypi --token $(UV_PUBLISH_TOKEN_TEST)


# ======================================================================================
# Pre-commit
# ======================================================================================
.PHONY: pre-commit-install
pre-commit-install: .pre-commit-config.yaml  ##@PreCommit Installs pre-commit hooks
	@echo "Installing pre-commit hooks..."
	@pre-commit install


# ======================================================================================
# Ruff
# ======================================================================================
.PHONY: linter
linter:  ##@Ruff Runs the Ruff linter on the project
	@echo "Running ruff linter..."
	@ruff check ./ --fix


.PHONY: formatter
formatter:  ##@Ruff Runs the Ruff formatter on the project
	@echo "Running ruff formatter..."
	@ruff format ./


.PHONY: ruff
ruff:  ##@Ruff Runs both the Ruff linter and formatter on the project
	@$(MAKE) linter
	@$(MAKE) formatter


# ======================================================================================
# MyPy
# ======================================================================================
.PHONY: mypy
mypy:  ##@Mypy Runs the MyPy static type checker on the project
	@echo "Running mypy..."
	@dmypy status > /dev/null 2>&1 || dmypy start
	@dmypy run -- ./ --install-types --non-interactive


# ======================================================================================
# PyTest
# ======================================================================================
.PHONY: pytest
pytest:  ##@Testing Runs the PyTest test suite
	@echo "Running pytest..."
	@pytest ./


# ======================================================================================
# Examples
# ======================================================================================
.PHONY: mock
mock:  ##@Examples Runs the Mock example
	@eventide run -a examples.mock:app --reload

.PHONY: sqs
sqs:  ##@Examples Runs the SQS example
	@eventide run -a examples.sqs:app --reload

.PHONY: cloudflare
cloudflare:  ##@Examples Runs the Cloudflare example
	@eventide run -a examples.cloudflare:app --reload


# ======================================================================================
# Help  -  https://stackoverflow.com/a/30796664
# ======================================================================================
HELP_FUN = \
    %help; while(<>){push@{$$help{$$2//'options'}},[$$1,$$3] \
    if/^([\w-_]+)\s*:.*\#\#(?:@(\w+))?\s(.*)$$/}; \
    print"$$_:\n", map"  $$_->[0]".(" "x(24-length($$_->[0])))."$$_->[1]\n",\
    @{$$help{$$_}},"\n" for sort keys %help; \

.PHONY: help
help: ##@Help Shows this help
	@echo "Usage: make [target] ..."
	@perl -e '$(HELP_FUN)' $(MAKEFILE_LIST)
