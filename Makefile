# Build, test, run, and operate the local object-store cluster.
COMPOSE := docker compose -f docker-compose.yml -f docker-compose.dev-ports.yml

.PHONY: build test vet fmt check up down logs demo clean smoke

# Compile the controller, worker, and CLI binaries into ./bin.
build:
	mkdir -p bin
	go build -o bin/controller ./cmd/controller
	go build -o bin/worker ./cmd/worker
	go build -o bin/ocs ./cmd/ocs

# Run the complete Go test suite with the race detector.
test:
	go test -race ./...

# Run static analysis for every Go package.
vet:
	go vet ./...

# Apply standard Go formatting to all tracked source files.
fmt:
	gofmt -w $$(find . -name '*.go' -not -path './.git/*')

# Run the checks required before sharing a change.
check: fmt vet test

# Build and start the local Docker Compose cluster.
up:
	$(COMPOSE) up -d --build

# Stop and remove the local Docker Compose cluster.
down:
	$(COMPOSE) down

# Follow logs from all Compose services.
logs:
	$(COMPOSE) logs -f

# Exercise the worker HTTP API, scrubber, persistence, and deletion behavior.
smoke:
	bash scripts/smoke.sh

# Run all end-to-end object-store demonstrations.
demo:
	bash scripts/demo/run_all.sh

# Remove local binaries and Go build cache artifacts.
clean:
	rm -rf bin
	go clean ./...
