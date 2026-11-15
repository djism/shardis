# Shardis build commands. Run `make help` to list them.
BIN     := bin/shardis
PKG     := ./...
GOFILES := $(shell find . -name '*.go' -not -path './website/*')

.DEFAULT_GOAL := help
.PHONY: help build test test-memory fuzz bench run fmt vet lint clean cluster-up cluster-down cluster-up-local cluster-down-local

help: ## list targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

build: ## build binaries into bin/
	go build -o $(BIN) ./cmd/shardis
	go build -o bin/shardis-loadgen ./cmd/shardis-loadgen
	go build -o bin/shardis-crashtest ./cmd/shardis-crashtest
	go build -o bin/shardis-admin ./cmd/shardis-admin

test: ## run all tests with the race detector
	go test -race -count=1 $(PKG)

test-memory: ## heap-measuring tests (skipped under -race in `make test`)
	go test -count=1 -run 'Memory' ./internal/store

fuzz: ## fuzz the RESP parser and the DUMP payload decoder (FUZZTIME=60s per target)
	@for f in FuzzReadCommand FuzzReadValue FuzzCommandRoundTrip; do \
		go test ./internal/resp -run '^$$' -fuzz "^$$f\$$" -fuzztime $${FUZZTIME:-60s} || exit 1; done
	go test ./internal/store -run '^$$' -fuzz '^FuzzParseDump$$' -fuzztime $${FUZZTIME:-60s}

bench: ## Go microbenchmarks
	go test -run '^$$' -bench . -benchmem ./...

run: build ## run a single node on :7000
	./$(BIN) --port 7000

fmt: ## format all Go code
	gofmt -w $(GOFILES)

vet: ## static checks
	go vet $(PKG)

lint: vet ## fail if any file is not gofmt-formatted
	@out=$$(gofmt -l $(GOFILES)); if [ -n "$$out" ]; then echo "needs gofmt:"; echo "$$out"; exit 1; fi

clean: ## remove build output
	rm -f $(BIN) bin/shardis-loadgen bin/shardis-crashtest bin/shardis-admin

cluster-up-local: ## 6-node cluster as local processes on 127.0.0.1:7000-7005
	deploy/local/cluster.sh up

cluster-down-local: ## stop the local cluster
	deploy/local/cluster.sh down

cluster-up: ## 6-node cluster with docker compose (clients connect to 127.0.0.1:7000-7005)
	docker compose -f deploy/compose/docker-compose.yml up -d --build
	@deploy/compose/wait.sh

cluster-down: ## stop the compose cluster and delete its data
	docker compose -f deploy/compose/docker-compose.yml down -v
