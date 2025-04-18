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
lock: .bootstrapped-mise pyproject.toml ##@UV Locks the Python dependencies
	@echo "Locking Python dependencies..."
	@uv lock --upgrade

.PHONY: sync
sync: .bootstrapped-mise pyproject.toml uv.lock ##@UV Installs the Python dependencies
	@echo "Installing Python dependencies..."
	@uv sync --all-extras --frozen --no-install-project

.PHONY: build
build: .bootstrapped-uv pyproject.toml uv.lock ##@UV Builds the package
	@echo "Building the package..."
	@uv build

.PHONY: publish
publish: .bootstrapped-uv pyproject.toml uv.lock ##@UV Publishes the package
	@echo "Publishing the package..."
	@uv publish


# ======================================================================================
# Pre-commit
# ======================================================================================
.PHONY: pre-commit-install
pre-commit-install: .bootstrapped-mise .pre-commit-config.yaml  ##@PreCommit Installs pre-commit hooks
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
	@dmypy run -- ./


# ======================================================================================
# PyTest
# ======================================================================================
.PHONY: pytest
pytest:  ##@Testing Runs the PyTest test suite
	@echo "Running pytest..."
	@pytest ./


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
