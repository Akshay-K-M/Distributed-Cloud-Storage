COMPOSE := docker compose -f docker-compose.yml -f docker-compose.dev-ports.yml

.PHONY: build test vet up down logs demo clean smoke

build:
	mkdir -p bin
	go build -o bin/controller ./cmd/controller
	go build -o bin/worker ./cmd/worker
	go build -o bin/ocs ./cmd/ocs

test:
	go test -race ./...

vet:
	go vet ./...

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

smoke:
	bash scripts/smoke.sh

demo:
	bash scripts/demo/run_all.sh

clean:
	rm -rf bin
	go clean ./...
