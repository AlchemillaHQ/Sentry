.PHONY: build test generate clean lint

BINARY_NAME=bin/sentry

build:
	mkdir -p bin
	go build -o $(BINARY_NAME) main.go

test:
	go test -v ./...

generate:
	$(go env GOPATH)/bin/sqlc generate

lint:
	go vet ./...
	staticcheck ./...

clean:
	rm -rf bin/
	go clean
