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
    git ls-files | grep '\.go$' | xargs gosimports -local github.com/block -w
    go mod tidy
