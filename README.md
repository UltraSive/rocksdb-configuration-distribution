# RocksDB Based Configuration Distribution Server
Persist state in key value pairs on disk and be able to proxy to full replicas if role indicates.

## Quick start (Docker)
Ensure Docker is installed on your system
```bash
curl -fsSL https://get.docker.com | sh
```

### Build The Container
```bash
make docker-build
```

### Run The Container
```bash
make docker-run
```

## Quick start (Ubuntu)

### Install System dependancies
```bash
sudo apt update
sudo apt install -y build-essential g++ git pkg-config wget \
    libgflags-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev
```

### Build and Install RocksDB from Source
```bash
# Clone RocksDB
git clone https://github.com/facebook/rocksdb.git
cd rocksdb

# Build RocksDB as shared library
make shared_lib -j$(nproc)

# Install it system-wide
sudo make install
```

### Configure Dymanic Linker
```bash
echo "/usr/local/lib" | sudo tee /etc/ld.so.conf.d/rocksdb.conf
sudo ldconfig
```
Verify with:
```bash
ldconfig -p | grep rocksdb
```
Output should be `/usr/local/lib/librocksdb.so.X.Y`.

### Clone The Project
```bash
# Go back to workspace
cd ~/projects
git clone https://github.com/UltraSive/rocksdb-configuration-distribution.git
cd rocksdb-configuration-distribution
```


# Build and Run
This repo includes a Makefile to simplify building and running.
```bash
# Build the Go binary
make build

# Run the server
make run

# Optional: Clean build artifacts
make clean
```