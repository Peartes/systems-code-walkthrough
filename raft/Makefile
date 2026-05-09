# Convenience targets for running the Raft demo locally.
#
# The cluster-shape (how many nodes) is set in the UI by hitting the
# Seed page once — that writes config.json. After that, `make nodes-up`
# spawns one raft-node process per peer in config.json, and `make
# nodes-down` kills them. The observability stack runs in Docker and
# is independent of the node lifecycle.
#
# Typical first-run dance:
#
#   make build          # build raft-node and raft-dashboard binaries
#   make observe        # bring up Prometheus + Grafana
#   make dashboard-up   # start the HTTP/JSON proxy on :8080
#   cd ui && npm run dev   # in another terminal, or just open the built dist
#   # In the browser at localhost:5173, seed the cluster (e.g. 3 nodes)
#   make nodes-up       # actually spawn the raft-node processes
#
# To clean up:  make all-down  (then `make clean` if you want a wipe).

SHELL := /bin/bash

GO     ?= go
BIN    := bin
DATA   := data

.PHONY: help build observe observe-down dashboard-up dashboard-down \
        nodes-up nodes-down all-down clean

help: ## Show this help.
	@awk 'BEGIN {FS = ":.*##"; printf "Targets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# ----- build --------------------------------------------------------------

build: ## Build raft-node and raft-dashboard binaries into bin/.
	@mkdir -p $(BIN)
	$(GO) build -o $(BIN)/raft-node ./cmd/node
	$(GO) build -o $(BIN)/raft-dashboard ./cmd/dashboard

# ----- observability stack (Prometheus + Grafana) -------------------------

observe: ## Bring up Prometheus + Grafana via docker-compose.
	docker compose -f deploy/observability/docker-compose.yml up -d
	@printf "\n  Grafana:    \033[36mhttp://localhost:13000\033[0m  (admin/admin)\n"
	@printf "  Prometheus: \033[36mhttp://localhost:9090\033[0m\n\n"

observe-down: ## Tear down the observability stack.
	docker compose -f deploy/observability/docker-compose.yml down

# ----- HTTP/JSON dashboard backend ---------------------------------------

dashboard-up: build ## Start the dashboard backend on :8080 (background).
	@mkdir -p $(DATA)
	@./$(BIN)/raft-dashboard --listen=:8080 --config=config.json > $(DATA)/dashboard.log 2>&1 &  echo $$! > $(DATA)/dashboard.pid
	@printf "  raft-dashboard pid=%s on \033[36mhttp://localhost:8080\033[0m\n" "$$(cat $(DATA)/dashboard.pid)"

dashboard-down: ## Stop the dashboard backend.
	@if [ -f $(DATA)/dashboard.pid ]; then \
		pid=$$(cat $(DATA)/dashboard.pid); \
		kill $$pid 2>/dev/null && echo "  stopped raft-dashboard (pid=$$pid)" || echo "  raft-dashboard already stopped"; \
		rm $(DATA)/dashboard.pid; \
	else \
		echo "  no dashboard pid file"; \
	fi

# ----- cluster nodes -----------------------------------------------------

nodes-up: build ## Spawn one raft-node per peer in config.json.
	@./scripts/nodes-up.sh

nodes-down: ## Kill all raft-node processes spawned via `nodes-up`.
	@./scripts/nodes-down.sh

# ----- aggregate ---------------------------------------------------------

all-down: nodes-down dashboard-down observe-down ## Stop everything.

clean: all-down ## Stop everything and remove bin/ and data/.
	rm -rf $(BIN) $(DATA)
