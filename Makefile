# ===========================================================================
# Makefile — data-pipeline orchestrator
#
# Usage:
#   make pipeline        # full run: build → init → up → trigger → wait → results
#   make up              # start Airflow services (assumes init already done)
#   make run             # trigger the DAG once (services must be running)
#   make wait            # poll until the latest run finishes
#   make results         # print output CSVs to terminal
#   make down            # stop all containers
#   make reset           # full teardown + wipe data
# ===========================================================================

COMPOSE       := docker compose --project-directory $(CURDIR)
EXEC          := $(COMPOSE) exec -T airflow-webserver
AIRFLOW       := $(EXEC) airflow
DAG_ID        := transactions_pipeline
WEBSERVER_URL := http://localhost:8080

.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------
.PHONY: help
help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*## "}{printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ---------------------------------------------------------------------------
# Infrastructure
# ---------------------------------------------------------------------------
.PHONY: setup
setup: ## Create host directories and .env (if missing)
	mkdir -p data/staging data/output logs plugins
	@if [ ! -f .env ]; then \
	  cp .env.example .env; \
	  echo "Created .env from .env.example — review the secrets before production use."; \
	fi

.PHONY: build
build: ## Build the custom Airflow Docker image
	$(COMPOSE) build

.PHONY: init
init: ## Initialise the Airflow DB and create the admin user
	$(COMPOSE) up airflow-init

.PHONY: up
up: ## Start webserver + scheduler in the background
	$(COMPOSE) up -d airflow-webserver airflow-scheduler
	@echo "Waiting for Airflow webserver to become healthy..."
	@for i in $$(seq 1 40); do \
	  STATUS=$$(curl -sf $(WEBSERVER_URL)/health \
	    | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('status',''))" 2>/dev/null); \
	  if [ "$$STATUS" = "healthy" ]; then \
	    echo "Airflow is ready at $(WEBSERVER_URL) (user: $(ADMIN_USER))"; \
	    break; \
	  fi; \
	  printf "."; sleep 3; \
	done

.PHONY: down
down: ## Stop all containers
	$(COMPOSE) down

# ---------------------------------------------------------------------------
# Pipeline execution
# ---------------------------------------------------------------------------
.PHONY: run
run: ## Trigger the transactions_pipeline DAG
	@echo "Triggering DAG '$(DAG_ID)'..."
	$(AIRFLOW) dags trigger $(DAG_ID)
	@echo "DAG triggered. Run 'make wait' to poll for completion."

.PHONY: wait
wait: ## Poll until the latest DAG run finishes (Ctrl-C to abort)
	@echo "Polling run state for '$(DAG_ID)' (Ctrl-C to abort)..."
	@while true; do \
	  STATE=$$($(AIRFLOW) dags list-runs --dag-id $(DAG_ID) -o json 2>/dev/null \
	    | python3 -c "import json,sys; lines=[l.strip() for l in sys.stdin if l.strip().startswith('[{') or l.strip()=='[]']; print(json.loads(lines[0])[0]['state'] if lines and json.loads(lines[0]) else 'no_runs')" 2>/dev/null \
	    || echo "unreachable"); \
	  printf "  state: $$STATE\n"; \
	  case "$$STATE" in \
	    success)    echo "  DAG run succeeded."; break ;; \
	    failed)     echo "  DAG run FAILED. Check 'make logs-scheduler'."; exit 1 ;; \
	    no_runs)    echo "  No run found yet — waiting..."; sleep 5 ;; \
	    unreachable) echo "  Container not ready yet..."; sleep 5 ;; \
	    *) sleep 5 ;; \
	  esac; \
	done

.PHONY: status
status: ## Show all DAG runs (tabular)
	$(AIRFLOW) dags list-runs --dag-id $(DAG_ID) -o table

.PHONY: results
results: ## Print the output CSVs to terminal
	@echo ""
	@echo "=== table1_region_risk.csv ==="
	@column -s, -t data/output/table1_region_risk.csv 2>/dev/null || cat data/output/table1_region_risk.csv
	@echo ""
	@echo "=== table2_top3_receivers.csv ==="
	@column -s, -t data/output/table2_top3_receivers.csv 2>/dev/null || cat data/output/table2_top3_receivers.csv
	@echo ""

# ---------------------------------------------------------------------------
# Logs
# ---------------------------------------------------------------------------
.PHONY: lock
lock: ## Regenerate poetry.lock inside the webserver container
	$(EXEC) sh -c "poetry lock --no-update && cp poetry.lock /opt/airflow/"

.PHONY: test
test: ## Run unit tests inside the webserver container
	$(EXEC) sh -c "poetry install --with dev --no-root --no-interaction --no-ansi -q && python -m pytest tests/unit -v"

.PHONY: test-cov
test-cov: ## Run unit tests with coverage report
	$(EXEC) sh -c "poetry install --with dev --no-root --no-interaction --no-ansi -q && python -m pytest tests/unit -v --cov=src --cov-report=term-missing --cov-report=html:htmlcov"

# ---------------------------------------------------------------------------
# Logs
# ---------------------------------------------------------------------------
.PHONY: logs-scheduler
logs-scheduler: ## Tail the scheduler container logs
	$(COMPOSE) logs --tail=80 -f airflow-scheduler

.PHONY: logs-webserver
logs-webserver: ## Tail the webserver container logs
	$(COMPOSE) logs --tail=40 -f airflow-webserver

# ---------------------------------------------------------------------------
# Maintenance
# ---------------------------------------------------------------------------
.PHONY: clean-data
clean-data: ## Remove staging and output artefacts (keeps raw data)
	rm -f data/staging/*.parquet data/output/*.parquet data/output/*.csv
	@echo "Staging and output data removed."

.PHONY: reset
reset: down clean-data ## Full reset: stop containers + wipe generated data
	rm -f airflow.db
	@echo "Reset complete. Run 'make init up' to restart from scratch."

# ---------------------------------------------------------------------------
# End-to-end shortcut
# ---------------------------------------------------------------------------
.PHONY: pipeline
pipeline: setup build init up run wait results ## Full pipeline: setup→build→init→up→trigger→wait→results
