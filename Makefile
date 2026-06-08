.PHONY: test test-integration coverage lint build generate

test:
	go test -race ./...

test-integration:
	go test -race -tags integration ./...

coverage:
	go test -coverprofile=unit.out -covermode=atomic ./...
	go test -tags integration -coverprofile=integration.out -covermode=atomic ./...
	go run github.com/wadey/gocovmerge@latest unit.out integration.out > coverage.out
	go tool cover -func=coverage.out
	go tool cover -html=coverage.out -o coverage.html

lint:
	go vet ./...
	go fmt ./...

build:
	go build -o bin/fluvio ./cmd/fluvio

generate:
	go generate ./...
