.PHONY: build test generate clean lint web-build web-dev

BINARY_NAME=bin/sentry
VERSION=v0.0.1
GIT_COMMIT=$(shell git rev-parse --short HEAD)
BUILD_TIME=$(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS=-ldflags "-X main.Version=$(VERSION) -X main.GitCommit=$(GIT_COMMIT) -X main.BuildTime=$(BUILD_TIME)"

NODE_BIN=$(shell find $(HOME)/.nvm/versions/node -name node -type f | head -n 1 | xargs dirname)
PATH_WITH_NODE=$(NODE_BIN):$(PATH)

build: web-build
	mkdir -p bin
	go build $(LDFLAGS) -o $(BINARY_NAME) main.go static.go

test:
	go test -v ./...

generate:
	$$(go env GOPATH)/bin/sqlc generate

lint:
	go vet ./...
	staticcheck ./...

web-install:
	cd web && PATH=$(PATH_WITH_NODE) npm install

web-build:
	cd web && PATH=$(PATH_WITH_NODE) npm run build

web-dev:
	cd web && PATH=$(PATH_WITH_NODE) npm run dev

clean:
	rm -rf bin/
	rm -rf web/build/
	go clean
