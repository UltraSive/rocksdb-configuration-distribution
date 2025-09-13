# Makefile for RocksDB Configuration Distribution Server

BINARY_NAME := bin/kvstore
CGO_CFLAGS  := -I/usr/local/include/rocksdb
CGO_LDFLAGS := -L/usr/local/lib -lrocksdb -lstdc++ -lm -lz -lsnappy -llz4 -lzstd
DOCKER_IMAGE := rocksdb-config-server
DOCKER_PORT  := 8080

.PHONY: all build run clean

all: build

build:
	@echo "🔨 Building $(BINARY_NAME)..."
	CGO_ENABLED=1 \
	CGO_CFLAGS="$(CGO_CFLAGS)" \
	CGO_LDFLAGS="$(CGO_LDFLAGS)" \
	go build -o $(BINARY_NAME) ./cmd/kvstore

run: build
	@echo "🚀 Running $(BINARY_NAME)..."
	LD_LIBRARY_PATH=/usr/local/lib ./$(BINARY_NAME)

clean:
	@echo "🧹 Cleaning build artifacts..."
	rm -f $(BINARY_NAME)

docker-build:
	@echo "🐳 Building Docker image $(DOCKER_IMAGE)..."
	docker build -t $(DOCKER_IMAGE) .

docker-run:
	@echo "🚀 Running Docker container $(DOCKER_IMAGE)..."
	docker run --rm -p $(DOCKER_PORT):$(DOCKER_PORT) $(DOCKER_IMAGE)