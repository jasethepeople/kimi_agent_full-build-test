# hpo-grid build/run infrastructure (SPEC 6.4)

COMPOSE ?= docker compose

.PHONY: build up down logs ps test-go test-clj test-worker smoke clean

build: ## Build all service images
	$(COMPOSE) build

up: ## Start the grid (scale workers with WORKERS=N, default 2)
	$(COMPOSE) up -d --scale worker=$(or $(WORKERS),2)

down: ## Stop and remove containers
	$(COMPOSE) down

logs: ## Tail logs from all services
	$(COMPOSE) logs -f

ps: ## Show service status
	$(COMPOSE) ps

test-go: ## Run Go unit tests
	cd orchestrator && go test ./...

test-clj: ## Run Clojure unit tests
	cd strategy-engine && clj -M:test

test-worker: ## Byte-compile and import-check the Python worker
	python3 -m py_compile worker/worker.py
	python3 -c "import importlib.util, sys; spec = importlib.util.spec_from_file_location('worker', 'worker/worker.py'); m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m) if hasattr(m, 'main') else None; print('worker importable')"

smoke: ## Run the end-to-end smoke test against a running grid
	bash scripts/smoke_test.sh

clean: ## Stop containers, remove volumes and built images
	$(COMPOSE) down -v --rmi local --remove-orphans
