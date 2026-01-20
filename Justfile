set positional-arguments := true
set shell := ["bash", "-c"]

_help:
    @just -l

# Run tests
test:
    @gotestsum --hide-summary output,skipped --format-hide-empty-pkg ${CI:+--format github-actions} ./... -- -race -timeout 30s

# Lint code
lint:
    #!/bin/bash
    golangci-lint run &
    actionlint &
    wait

# Format code
fmt:
    #!/bin/bash
    just --unstable --fmt &
    golangci-lint fmt &
    go mod tidy &
    wait
