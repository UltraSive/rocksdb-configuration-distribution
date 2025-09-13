package main

import (
    "context"
    "encoding/json"
    "fmt"
    "net"
    "net/http"
    "os"
    "os/signal"
    "time"

    "github.com/UltraSive/rocksdb-configuration-distribution/internal/cleaner"
    "github.com/UltraSive/rocksdb-configuration-distribution/internal/datastore"
    "github.com/UltraSive/rocksdb-configuration-distribution/internal/handler"
    "github.com/UltraSive/rocksdb-configuration-distribution/internal/transport"
    "github.com/UltraSive/rocksdb-configuration-distribution/internal/upstream"
)

func main() {
    // --- Config ---
    socketPath := "/tmp/kvstore.sock" // could make this configurable via env
    upstreamURL := os.Getenv("UPSTREAM_URL")
    ttl := 30 * time.Second           // default TTL (0 = infinite)
    janitorInterval := 60 * time.Second

    // --- RocksDB Setup ---
    db, err := datastore.NewRocksDB("./kvdb")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // --- Upstream Client ---
    var up *upstream.Client
    if upstreamURL != "" {
        up = upstream.New(upstreamURL, 5*time.Second)
    }

    // --- Handler ---
    h := handler.New(db, up, ttl)

    // --- Serve Function (used by HTTP + Unix transport) ---
    serveFn := func(payload []byte) ([]byte, error) {
        var req handler.Request
        if err := json.Unmarshal(payload, &req); err != nil {
            return nil, err
        }
        resp := h.Serve(req)
        return json.Marshal(resp)
    }

		// Remove old socket if it exists
		if _, err := os.Stat(socketPath); err == nil {
				os.Remove(socketPath)
		}

    // --- Start Unix Socket Listener ---
    go func() {
        if err := transport.ServeUnix(socketPath, func(conn net.Conn) {
            defer conn.Close()

            // Read request
            msg, err := transport.ReadMessage(conn)
            if err != nil {
                fmt.Println("error reading:", err)
                return
            }

            // Process
            resp, err := serveFn(msg)
            if err != nil {
                fmt.Println("handler error:", err)
                return
            }

            // Write response
            if err := transport.WriteMessage(conn, resp); err != nil {
                fmt.Println("error writing:", err)
            }
        }); err != nil {
            fmt.Println("unix socket server error:", err)
        }
    }()

    // --- Start HTTP Server ---
    httpSrv := &http.Server{
        Addr:    ":8080",
        Handler: transport.NewHTTPRouter(serveFn),
    }
    go func() {
        if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            fmt.Println("http server error:", err)
        }
    }()

    // --- Start Cleaner (only if TTL > 0) ---
    stopCleaner := make(chan struct{})
    if ttl > 0 {
        cleaner.Start(db, janitorInterval, 1000, stopCleaner)
    }

    // --- Wait for Interrupt ---
    stop := make(chan os.Signal, 1)
    signal.Notify(stop, os.Interrupt)
    <-stop
    fmt.Println("shutting down...")

    // Stop cleaner gracefully
    if ttl > 0 {
        close(stopCleaner)
    }

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    _ = httpSrv.Shutdown(ctx)
    fmt.Println("shutdown complete")
}
