# Shardis server image: a static Go binary on a minimal, non-root base.
# Build: docker build -t shardis:dev .
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/shardis ./cmd/shardis && mkdir -p /out/data

# distroless: no shell, no package manager, runs as uid 65532.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/shardis /shardis
COPY --from=build --chown=65532:65532 /out/data /data
EXPOSE 7000 17000
ENTRYPOINT ["/shardis"]
