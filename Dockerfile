# Base image
FROM ubuntu:24.04

# Set non-interactive
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    g++ \
    git \
    wget \
    pkg-config \
    libgflags-dev \
    libsnappy-dev \
    zlib1g-dev \
    libbz2-dev \
    liblz4-dev \
    libzstd-dev \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables for Go/CGO
ENV CGO_ENABLED=1 \
    CGO_CFLAGS="-I/usr/local/include/rocksdb" \
    CGO_LDFLAGS="-L/usr/local/lib -lrocksdb -lstdc++ -lm -lz -lsnappy -llz4 -lzstd"

# Install Go
ENV GO_VERSION=1.24.3
RUN wget -q https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz -O /tmp/go.tar.gz \
    && tar -C /usr/local -xzf /tmp/go.tar.gz \
    && rm /tmp/go.tar.gz
ENV PATH="/usr/local/go/bin:${PATH}"

# Clone and build RocksDB from source
RUN git clone https://github.com/facebook/rocksdb.git /tmp/rocksdb \
    && cd /tmp/rocksdb \
    && make shared_lib -j$(nproc) \
    && make install-shared \
    && rm -rf /tmp/rocksdb

# Configure dynamic linker
RUN echo "/usr/local/lib" > /etc/ld.so.conf.d/rocksdb.conf \
    && ldconfig

# Set working directory for your project
WORKDIR /app

# Copy project files into container
COPY . .

# Build your Go project using Makefile
RUN make build

# Expose default port (adjust if needed)
EXPOSE 8080

# Run the server
CMD ["make", "run"]
