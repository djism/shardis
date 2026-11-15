# Shardis image: static Go binaries on a minimal, non-root base.
# Build: docker build -t shardis:dev .     (on minikube: minikube image build -t shardis:dev .)
#
# /shardis         the server (default entrypoint)
# /shardis-admin   cluster operations: create (the Kubernetes bootstrap Job), add-node, rebalance, ...
# /shardis-chaos   workload + consistency checker (-external mode runs as a Kubernetes Job)
# /shardis-loadgen load generator
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN for c in shardis shardis-admin shardis-chaos shardis-loadgen; do \
      CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/$c ./cmd/$c || exit 1; \
    done && mkdir -p /out/data

# distroless: no shell, no package manager, runs as uid 65532.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/shardis /out/shardis-admin /out/shardis-chaos /out/shardis-loadgen /
COPY --from=build --chown=65532:65532 /out/data /data
EXPOSE 7000 17000
ENTRYPOINT ["/shardis"]
