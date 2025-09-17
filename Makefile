# Makefile for RocksDB Configuration Distribution Server

BINARY_NAME := bin/kvstore
DOCKER_IMAGE := config-server
DOCKER_PORT  := 8080

.PHONY: all build run clean

all: build

build:
	@echo "🔨 Building $(BINARY_NAME)..."
	go build -o $(BINARY_NAME) .

run: build
	@echo "🚀 Running $(BINARY_NAME)..."
	./$(BINARY_NAME)

clean:
	@echo "🧹 Cleaning build artifacts..."
	rm -f $(BINARY_NAME)

docker-build:
	@echo "🐳 Building Docker image $(DOCKER_IMAGE)..."
	docker build -t $(DOCKER_IMAGE) .

docker-run:
	@echo "🚀 Running Docker container $(DOCKER_IMAGE)..."
	docker run --rm -p $(DOCKER_PORT):$(DOCKER_PORT) $(DOCKER_IMAGE)