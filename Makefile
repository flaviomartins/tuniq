BINARY=tuniq
PKG=./...
LDFLAGS=-X github.com/flaviomartins/tuniq.version=$(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

.PHONY: build test fmt vet lint clean release

build:
	go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY) .

test:
	go test $(PKG)

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './.git/*')

vet:
	go vet $(PKG)

lint: fmt vet test

clean:
	rm -rf bin dist

release: clean
	mkdir -p dist
	GOOS=linux GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o dist/$(BINARY)-linux-amd64 .
	GOOS=linux GOARCH=arm64 go build -ldflags "$(LDFLAGS)" -o dist/$(BINARY)-linux-arm64 .
	GOOS=darwin GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o dist/$(BINARY)-darwin-amd64 .
	GOOS=darwin GOARCH=arm64 go build -ldflags "$(LDFLAGS)" -o dist/$(BINARY)-darwin-arm64 .
	GOOS=windows GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o dist/$(BINARY)-windows-amd64.exe .
	GOOS=windows GOARCH=arm64 go build -ldflags "$(LDFLAGS)" -o dist/$(BINARY)-windows-arm64.exe .
