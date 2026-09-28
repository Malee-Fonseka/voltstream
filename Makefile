# voltstream — the graded entry point is `make demo` (Appendix B, T110).
#
# Every target that starts the stack passes `--env-file`, because Compose looks for
# `.env` beside the compose file (docker/) rather than at the repository root. Without it
# the simulated-clock anchor resolves to empty and each container anchors on its own start
# time, which puts them minutes of simulated time apart.
#
# `up` waits on health conditions and never sleeps (§8.2). `docker compose up --wait`
# blocks until every service with a healthcheck reports healthy, so a demo cannot start
# querying an API whose database has not finished applying its schema.

SHELL := /bin/bash
COMPOSE := docker compose --env-file .env -f docker/docker-compose.yml
PY := .venv/Scripts/python.exe
ifeq (,$(wildcard .venv/Scripts/python.exe))
PY := .venv/bin/python
endif


.PHONY: help up demo down clean test test-all lint check-alerts logs faults backfill kill-test anchor

help:
	@echo "voltstream targets:"
	@echo "  make up          bring the stack up and wait for health"
	@echo "  make demo        start, show a provisional bill, wait for it to finalise, show the delta"
	@echo "  make down        stop the stack, keep the data"
	@echo "  make clean       stop the stack and DESTROY volumes"
	@echo "  make test        unit, property and consistency tests"
	@echo "  make test-all    everything, including integration (needs the stack up)"
	@echo "  make lint        ruff and mypy"
	@echo "  make check-alerts  validate the alert rules and Alertmanager config, run the rule tests"
	@echo "  make logs s=api  follow one service's logs"
	@echo "  make faults      break the pipeline and assert each alert fires   [s=stale|rejects|sla|renewable|divergence]"
	@echo "  make backfill d=2026-01-02   restate one simulated day"
	@echo "  make kill-test   kill the speed layer mid-day, show recovery and unaffected bills"

# Stamp a fresh clock anchor into .env. Every start goes through this: the anchor is a
# real instant that simulated time is measured from, scaled by 288, so one left over from
# yesterday puts the simulation months out (see simclock.py).
anchor:
	@test -f .env || cp .env.example .env
	@ANCHOR=$$(date -u +%Y-%m-%dT%H:%M:%SZ); \
	 if grep -q '^VOLTSTREAM_ANCHOR_REAL=' .env; then \
	   sed -i "s|^VOLTSTREAM_ANCHOR_REAL=.*|VOLTSTREAM_ANCHOR_REAL=$$ANCHOR|" .env; \
	 else \
	   echo "VOLTSTREAM_ANCHOR_REAL=$$ANCHOR" >> .env; \
	 fi; \
	 echo "clock anchored at $$ANCHOR"

up: anchor
	$(COMPOSE) up -d --wait
	@echo
	@echo "stack up."
	@echo "  API      http://localhost:8000/docs"
	@echo "  MinIO    http://localhost:9001   (voltstream / voltstream-dev)"
	@echo "  metrics  http://localhost:8011/metrics  (archiver)"
	@echo "           http://localhost:8012/metrics  (speed layer)"
	@echo "  Grafana  http://localhost:3000   (no login; three dashboards)"
	@echo "  Prometheus http://localhost:9090  Alertmanager http://localhost:9093"

# T163. demo.sh starts the stack itself (keeping the clock of a stack that already holds
# data, which `up` would re-anchor) and needs no human input.
demo:
	bash scripts/demo.sh

down:
	$(COMPOSE) down

# Destroys the volumes, which includes the Spark checkpoints. The next start therefore
# replays from the beginning of the topic rather than resuming — intended, but it is the
# difference between `down` and `clean`.
clean:
	$(COMPOSE) down -v

test:
	$(PY) -m pytest -m "not integration"

test-all:
	$(PY) -m pytest

lint:
	$(PY) -m ruff check src tests
	$(PY) -m ruff format --check src tests
	$(PY) -m mypy src
	@# D7: no identifier may use the misspelling. Comment lines are excluded, because the
	@# places that legitimately mention it are explaining exactly this decision — the
	@# compose header saying why `name: voltstream` is mandatory, and the README's note.
	@# What the rule is for is a misspelled container, volume, package or column name, and
	@# those cannot live in a comment.
	@if grep -rni "volstream" src/ docker/ config/ airflow/ scripts/ tests/ 2>/dev/null \
	     | grep -vE ':\s*(#|--|//)' ; then \
	  echo "found 'volstream' (missing a t) in an identifier — see D7"; \
	  exit 1; \
	fi
	@echo "lint clean"

# The alerting configuration, checked by the tools that will load it, in the images Compose
# pins (so the checker is the same version as the server). `promtool check config` also
# checks the rule file it references; `test rules` runs alert_rules.test.yml against
# synthetic series. Needs Docker, not the running stack.
check-alerts:
	$(COMPOSE) run --rm --no-deps --entrypoint promtool prometheus check config /etc/prometheus/prometheus.yml
	$(COMPOSE) run --rm --no-deps --entrypoint promtool prometheus test rules /etc/prometheus/alert_rules.test.yml
	$(COMPOSE) run --rm --no-deps --entrypoint amtool alertmanager check-config /etc/alertmanager/alertmanager.yml

logs:
	@test -n "$(s)" || { echo "usage: make logs s=<service>"; exit 1; }
	$(COMPOSE) logs -f $(s)

faults:
	bash scripts/inject_faults.sh $(or $(s),all)

backfill:
	@test -n "$(d)" || { echo "usage: make backfill d=YYYY-MM-DD"; exit 1; }
	bash scripts/backfill.sh $(d)

kill-test:
	bash scripts/smoke_test.sh kill-speed-layer
