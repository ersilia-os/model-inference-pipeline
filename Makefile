PYTHON_BINARY := python3
PROJECT_NAME := precalculator
TEST_DIR := tests
VIRTUAL_ENV := .venv
VIRTUAL_BIN := $(VIRTUAL_ENV)/bin
.DEFAULT_GOAL := help
.PHONY: help install clean black lint format test hooks install-hooks install-ersilia

## help - Display help about make targets for this Makefile
help:
	@cat Makefile | grep '^## ' --color=never | cut -c4- | sed -e "`printf 's/ - /\t- /;'`" | column -s "`printf '\t'`" -t

## install - Install virtual environment for DEV use
install:
	@echo "Installing..."
	@if [ "$(shell which poetry)" = "" ]; then \
		$(MAKE) install-poetry; \
	fi
	@$(MAKE) install-ersilia setup-poetry install-hooks

install-prod:
	@if [ "$(shell which poetry)" = "" ]; then \
		$(MAKE) install-poetry; \
	fi
	@$(MAKE) install-ersilia
	@poetry env use python3 && poetry install --without dev

install-poetry:
	@echo "Installing poetry..."
	@curl -sSL https://install.python-poetry.org | python3 -
	@export PATH="$HOME/.local/bin:$PATH"

setup-poetry:
	@poetry env use python3 && poetry install

## clean - Remove the virtual environment and clear out .pyc files
clean:
	rm -rf $(VIRTUAL_ENV) dist *.egg-info .coverage ersilia
	find . -name '*.pyc' -delete

## black - Runs the Black Python formatter against the project
black:
	$(VIRTUAL_BIN)/black $(PROJECT_NAME)/ $(TEST_DIR)/ scripts/ workflows/

## format - Runs all formatting tools against the project
format: black lint

## lint - Lint the project
lint:
	$(VIRTUAL_BIN)/ruff $(PROJECT_NAME)/ $(TEST_DIR)/ scripts/ workflows/

## mypy - Run mypy type checking on the project
mypy:
	$(VIRTUAL_BIN)/mypy $(PROJECT_NAME)/ $(TEST_DIR)/ scripts/ workflows/

## test - Test the project
test:
	$(VIRTUAL_BIN)/pytest $(TEST_DIR)/

# fetch ersilia model hub repo
install-ersilia:
	@if [ ! -d "./ersilia" ]; then \
		git clone https://github.com/ersilia-os/ersilia.git@fix/isaura-dependecy-fix && poetry install; \
	fi

## hooks - run pre-commit git hooks on all files
hooks: setup-poetry
	@$(VIRTUAL_ENV)/bin/pre-commit run --show-diff-on-failure --color=always --all-files --hook-stage push

install-hooks: .git/hooks/pre-commit .git/hooks/pre-push
	$(VIRTUAL_ENV)/bin/pre-commit install-hooks
	
.git/hooks/pre-commit:
	$(VIRTUAL_ENV)/bin/pre-commit install --hook-type pre-commit

.git/hooks/pre-push: 
	$(VIRTUAL_ENV)/bin/pre-commit install --hook-type pre-push
