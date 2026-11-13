# Shardis build commands. Run `make help` to list them.
BIN     := bin/shardis
PKG     := ./...
GOFILES := $(shell find . -name '*.go' -not -path './website/*')

.DEFAULT_GOAL := help
.PHONY: help build test run fmt vet lint clean cluster-up cluster-down

help: ## list targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

build: ## build binaries into bin/
	go build -o $(BIN) ./cmd/shardis

test: ## run all tests with the race detector
	go test -race -count=1 $(PKG)

run: build ## run a single node on :7000
	./$(BIN) --port 7000

fmt: ## format all Go code
	gofmt -w $(GOFILES)

vet: ## static checks
	go vet $(PKG)

lint: vet ## fail if any file is not gofmt-formatted
	@out=$$(gofmt -l $(GOFILES)); if [ -n "$$out" ]; then echo "needs gofmt:"; echo "$$out"; exit 1; fi

clean: ## remove build output
	rm -f $(BIN)

cluster-up: ## 6-node cluster with docker compose (Iteration 7)
	@echo "cluster-up arrives in Iteration 7 (deploy/compose)"

cluster-down: ## stop the compose cluster
	@echo "cluster-down arrives in Iteration 7 (deploy/compose)"
