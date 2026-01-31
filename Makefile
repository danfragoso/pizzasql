.PHONY: build test test-v test-cover bench clean fmt lint

# Build the project
build:
	go build -o ./bin/pizzasql ./main.go

build-linux-amd64:
	GOOS=linux GOARCH=amd64 go build -o ./bin/pizzasql-linux-amd64 ./main.go

# Run all tests
test:
	go test ./...

# Run tests with verbose output
test-v:
	go test -v ./...

# Run tests with coverage
test-cover:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# Run benchmarks
bench:
	go test -bench=. -benchmem ./...

# Run lexer tests only
test-lexer:
	go test -v ./pkg/lexer/...

# Run parser tests only
test-parser:
	go test -v ./pkg/parser/...

# Format code
fmt:
	go fmt ./...

# Run linter (requires golangci-lint)
lint:
	golangci-lint run

# Clean build artifacts
clean:
	rm -f pizzasql coverage.out coverage.html

# Run tests with race detection
test-race:
	go test -race ./...

# Quick test for development
quick:
	go test -short ./...
