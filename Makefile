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

# One simulated day is 5 real minutes; give the demo a day plus margin to close one.
DEMO_SECONDS ?= 420

.PHONY: help up demo down clean test test-all lint logs faults backfill anchor

help:
	@echo "voltstream targets:"
	@echo "  make up          bring the stack up and wait for health"
	@echo "  make demo        up, run one simulated day, print where to look"
	@echo "  make down        stop the stack, keep the data"
	@echo "  make clean       stop the stack and DESTROY volumes"
	@echo "  make test        unit, property and consistency tests"
	@echo "  make test-all    everything, including integration (needs the stack up)"
	@echo "  make lint        ruff and mypy"
	@echo "  make logs s=api  follow one service's logs"
	@echo "  make faults      trigger each alert in turn"
	@echo "  make backfill d=2026-01-02   restate one simulated day"

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

demo: up
	@echo
	@echo "running one simulated day ($(DEMO_SECONDS)s at 288x)..."
	@sleep $(DEMO_SECONDS)
	@echo
	@echo "zone load now:"
	@curl -s http://localhost:8000/api/v1/zones/load | head -c 2000 || true
	@echo
	@echo "open http://localhost:8000/docs to explore."

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

logs:
	@test -n "$(s)" || { echo "usage: make logs s=<service>"; exit 1; }
	$(COMPOSE) logs -f $(s)

faults:
	bash scripts/inject_faults.sh

backfill:
	@test -n "$(d)" || { echo "usage: make backfill d=YYYY-MM-DD"; exit 1; }
	bash scripts/backfill.sh $(d)
