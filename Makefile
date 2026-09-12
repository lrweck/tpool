POSTGRES_DSN ?= postgres://postgres:postgres@localhost:5432/app

.PHONY: db-up db-down db-reset unit vet test-it test-it-quiet

db-up:
	docker compose up -d postgres

db-down:
	docker compose down

# nuke the volume: recreates a fresh DB with the new max_connections
db-reset:
	docker compose down -v

vet:
	go vet ./...

unit:
	go vet ./... && go test -race ./...

test-it: db-up
	DATABASE_URL=$(POSTGRES_DSN) go test -tags integration -v -count=1 -timeout 5m ./...

test-it-quiet: db-up
	DATABASE_URL=$(POSTGRES_DSN) go test -tags integration -count=1 -timeout 5m ./...