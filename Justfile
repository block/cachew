_help:
    @just -l

# Run tests
test:
    go test ./...

# Lint code
lint:
    golangci-lint run
    actionlint

# Format code
fmt:
    just --unstable --fmt
    golangci-lint fmt
    go mod tidy
