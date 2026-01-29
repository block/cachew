# Contributing to Cachew

Thank you for your interest in contributing to Cachew! This guide will help you get started with local development, building, and testing.

## Local Development

**Run natively (fastest for development):**
```bash
just run                    # Build and run on localhost:8080
```

**Run in Docker:**
```bash
just docker run             # Build and run in container
just docker run debug       # Run with debug logging
```

## Building and Testing

```bash
just build              # Build for current platform
just build-all          # Build all platforms
just build GOOS=linux GOARCH=amd64 # Build for linux/amd64
just test               # Run tests
just lint               # Lint code
just fmt                # Format code
```

## Docker

```bash
just docker build           # Build single-arch Docker image for local use
just docker build-multi     # Build multi-arch image (amd64 + arm64)
just docker run             # Run in container
just docker run debug       # Run with debug logging
just docker clean           # Clean up Docker images
```

## Using the Cache

The `cachew` CLI client interacts with the `cachewd` server:

```bash
# Upload to cache
cachew put my-key myfile.txt --ttl 24h

# Download from cache  
cachew get my-key -o myfile.txt

# Check if cached
cachew stat my-key

# Snapshot a directory
cachew snapshot deps-cache ./node_modules --ttl 7d

# Restore a directory
cachew restore deps-cache ./node_modules
```
