# -------- Stage 1: Build --------
FROM golang:1.23 AS builder

# Set working directory
WORKDIR /app

# Copy go.mod and go.sum first (for caching dependencies)
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source
COPY . .

# Build statically linked binary
RUN CGO_ENABLED=0 GOOS=linux go build -o server .

# -------- Stage 2: Runtime --------
FROM gcr.io/distroless/base-debian12

# Set working directory
WORKDIR /app

# Copy compiled binary from builder
COPY --from=builder /app/server .

# Create a directory for BadgerDB data
VOLUME ["/data"]
ENV BADGER_DIR=/data

# Expose HTTP port (adjust if needed)
EXPOSE 8080

# Run the binary
ENTRYPOINT ["./server"]
