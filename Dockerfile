# syntax=docker/dockerfile:1
# Multi-stage image for the controller, worker, and CLI binaries.
FROM golang:alpine AS build
WORKDIR /src
RUN apk add --no-cache git ca-certificates
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
ENV CGO_ENABLED=0
RUN go build -trimpath -ldflags="-s -w" -o /out/controller ./cmd/controller \
 && go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker \
 && go build -trimpath -ldflags="-s -w" -o /out/ocs ./cmd/ocs

FROM alpine:3.20 AS controller
RUN apk add --no-cache ca-certificates wget \
 && adduser -D -u 1000 app \
 && mkdir -p /data \
 && chown app:app /data
COPY --from=build /out/controller /usr/local/bin/controller
USER app
EXPOSE 8080
VOLUME ["/data"]
HEALTHCHECK --interval=5s --timeout=3s --retries=10 CMD wget -qO- http://localhost:8080/healthz || exit 1
ENTRYPOINT ["/usr/local/bin/controller"]

FROM alpine:3.20 AS worker
RUN apk add --no-cache ca-certificates wget \
 && adduser -D -u 1000 app \
 && mkdir -p /data \
 && chown app:app /data
COPY --from=build /out/worker /usr/local/bin/worker
USER app
EXPOSE 9000
VOLUME ["/data"]
HEALTHCHECK --interval=5s --timeout=3s --retries=10 CMD wget -qO- http://localhost:9000/healthz || exit 1
ENTRYPOINT ["/usr/local/bin/worker"]

FROM alpine:3.20 AS client
RUN apk add --no-cache ca-certificates wget \
 && adduser -D -u 1000 app
COPY --from=build /out/ocs /usr/local/bin/ocs
USER app
ENTRYPOINT ["/usr/local/bin/ocs"]
