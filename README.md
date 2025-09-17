# RocksDB Based Configuration Distribution Server
This is a configuration distribution server that stores and serves key-value data for clients over HTTP or Unix sockets. It provides fast local access while keeping persistent storage on disk, allowing updates, reads, and listing of stored items. The system is designed to handle multiple clients efficiently and ensures data consistency, making it suitable for distributing configuration or metadata across applications.

## Quick start (Docker)
Ensure Docker is installed on your system
```bash
curl -fsSL https://get.docker.com | sh
```

### Build and Run
```bash
# Build the container
make docker-build

# Run the container
make docker-run
```

## Quick start (Ubuntu)

### Clone The Project
```bash
# Go back to workspace
cd ~/projects
git clone https://github.com/UltraSive/configuration-distribution.git
cd configuration-distribution
```


## Build and Run
This repo includes a Makefile to simplify building and running.
```bash
# Build the Go binary
make build

# Run the server
make run

# Optional: Clean build artifacts
make clean
```

## API Examples

### Insert or Update Key/Value Pairs
This will create or overwrite keys with new values.
```bash
curl -X POST http://localhost:8080/ \
  -H "Content-Type: application/json" \
  -d '{
    "type": "UPDATE",
    "items": {
      "foo": {"bar": 123},
      "hello": "world"
    }
  }'
```
Response:
```bash
{
  "type": "OK"
}
```

### Read Keys
Request one or more keys.
If keys are not found, they will return null.
```bash
curl -X POST http://localhost:8080/ \
  -H "Content-Type: application/json" \
  -d '{
    "type": "GET",
    "keys": ["foo", "hello", "missingKey"]
  }'
```
Response:
```bash
{
  "type": "OK",
  "data": {
    "foo": {"bar": 123},
    "hello": "world",
    "missingKey": null
  }
}
```

### List All Keys
Returns the full key/value set in the database (filtered by TTL if running in ephemeral mode).
```bash
curl -X POST http://localhost:8080/ \
  -H "Content-Type: application/json" \
  -d '{"type": "LIST"}'
```
Response:
```bash
{
  "type": "OK",
  "data": {
    "foo": {"bar": 123},
    "hello": "world"
  }
}
```

### Delete a Key
To delete, send an empty value for the key inside an UPDATE request.
```bash
curl -X POST http://localhost:8080/ \
  -H "Content-Type: application/json" \
  -d '{
    "type": "UPDATE",
    "items": {
      "hello": ""
    }
  }'
```
Response:
```bash
{
  "type": "OK"
}
```