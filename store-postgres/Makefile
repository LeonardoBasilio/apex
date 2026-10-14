# store-postgres — portfolio-port infra. `make help` lists targets.
SHELL := /usr/bin/env bash

.DEFAULT_GOAL := help
.PHONY: help up down logs psql apply-ddl smoke test

help: ## List available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n",$$1,$$2}'

up: ## Start kafka + postgres + consumer (builds the consumer image if needed)
	docker compose up -d --wait

down: ## Stop the stack, preserving volumes
	docker compose down

logs: ## Tail the consumer's logs
	docker compose logs -f consumer

psql: ## Open a psql shell against the running postgres
	docker compose exec postgres psql -U $${POSTGRES_USER:-apex} -d $${POSTGRES_DB:-apex}

apply-ddl: ## Idempotently re-apply every ddl/*.sql against the RUNNING postgres
	docker compose exec -T postgres psql -U $${POSTGRES_USER:-apex} -d $${POSTGRES_DB:-apex} \
	  -f /docker-entrypoint-initdb.d/001_stage_events.sql

smoke: ## Produce one fake stage event and confirm it lands in postgres (needs `up` first)
	POSTGRES_DSN=postgresql://$${POSTGRES_USER:-apex}:$${POSTGRES_PASSWORD:-apex_local_dev}@localhost:$${POSTGRES_HOST_PORT:-5432}/$${POSTGRES_DB:-apex} \
	KAFKA_BOOTSTRAP_SERVERS=localhost:$${KAFKA_HOST_PORT:-9094} \
	  python -m pytest tests/test_integration_live.py -v

test: ## Run the lane's own unit tests (no infra needed)
	python -m pytest -q
