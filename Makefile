.PHONY: observability-up observability-down test smoke report compare evidence-bundle validate

PYTHON ?= $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)
RESULTS ?=

observability-up:
	docker compose up -d

observability-down:
	docker compose down

test:
	$(PYTHON) -m unittest discover -s tests -v

validate: test
	$(PYTHON) -m ruff format --check .
	$(PYTHON) -m py_compile cdc_lab.py analyze.py compare.py report.py destination.py metrics.py clickpipe_metrics.py
	$(PYTHON) -m json.tool observability/grafana/dashboards/pg-cdc-lab.json >/dev/null
	docker compose config -q

smoke:
	$(PYTHON) cdc_lab.py run --outcome commit --rate 20 --workers 4 --baseline-seconds 30 --large-rows 10000 --hold-seconds 10 --recovery-seconds 60

report:
	test -n "$(RESULTS)"
	$(PYTHON) analyze.py "$(RESULTS)"

compare:
	test -n "$(RESULTS)"
	$(PYTHON) compare.py $(RESULTS)

evidence-bundle:
	test -n "$(RESULTS)"
	$(PYTHON) report.py $(RESULTS)
