APP_DIR := $(patsubst %/,%,$(dir $(realpath $(lastword $(MAKEFILE_LIST)))))
SHELL := /bin/bash
PYTHON_ENV := venv
PROJECT_NAME := moto
IN_ENV=. $(PYTHON_ENV)/bin/activate
VIRTUAL_ENV := $(shell virtualenv --version)


.PHONY: env
env: $(PYTHON_ENV)

.PHONY: clean
clean:
	@find $(APP_DIR) -name "*.pyc" -exec rm -f {} \;

$(PYTHON_ENV):
	@virtualenv --no-site-packages --prompt='($(PROJECT_NAME))' $(PYTHON_ENV)
	@$(IN_ENV); pip install -U wheel pip && pip install -U -r requirements-dev.txt
	@echo ". $(PYTHON_ENV)/bin/activate"

.PHONY: init
init:
	@python setup.py develop
	@pip install -r requirements.txt

.PHONY: test
test: env
	@rm -f .coverage
	@$(PYTHON_ENV)/bin/nosetests -sv --with-coverage ./tests/

.PHONY: publish
publish:
	python setup.py sdist bdist_wheel upload
