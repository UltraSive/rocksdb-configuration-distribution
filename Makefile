# Makefile for RocksDB Configuration Distribution Server

BINARY_NAME := rocksdb-server
CGO_CFLAGS  := -I/usr/local/include/rocksdb
CGO_LDFLAGS := -L/usr/local/lib -lrocksdb -lstdc++ -lm -lz -lsnappy -llz4 -lzstd

.PHONY: all build run clean

all: build

build:
	@echo "🔨 Building $(BINARY_NAME)..."
	CGO_ENABLED=1 \
	CGO_CFLAGS="$(CGO_CFLAGS)" \
	CGO_LDFLAGS="$(CGO_LDFLAGS)" \
	go build -o $(BINARY_NAME) .

run: build
	@echo "🚀 Running $(BINARY_NAME)..."
	LD_LIBRARY_PATH=/usr/local/lib ./$(BINARY_NAME)

clean:
	@echo "🧹 Cleaning build artifacts..."
	rm -f $(BINARY_NAME)
