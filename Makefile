.PHONY: test test-integration lint build generate

test:
	go test -race ./...

test-integration:
	go test -race -tags integration ./...

lint:
	go vet ./...
	go fmt ./...

build:
	go build -o bin/fluvio ./cmd/fluvio

generate:
	go generate ./...
