PYTHON ?= python3
export PYTHONPATH := src

.PHONY: test run reconcile ci

test:
	$(PYTHON) -m pytest

run:
	$(PYTHON) -m etl_validation run

reconcile:
	$(PYTHON) -m etl_validation reconcile

ci:
	mkdir -p reports
	$(PYTHON) -m pytest --junitxml=reports/junit.xml --html=reports/pytest.html --self-contained-html
	$(PYTHON) -m etl_validation run
	$(PYTHON) -m etl_validation reconcile
