package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/linxGnu/grocksdb"
)

type Request struct {
	Type  string                     `json:"type"`
	Keys  []string                   `json:"keys,omitempty"`
	Items map[string]json.RawMessage `json:"items,omitempty"`
}

type Response struct {
	Type  string                 `json:"type"`
	Error string                 `json:"error,omitempty"`
	Data  map[string]interface{} `json:"data,omitempty"`
}

// dbEntry is the wrapper stored in RocksDB
type dbEntry struct {
	Expiry int64           `json:"expiry"` // unix nano
	Value  json.RawMessage `json:"value"`
}

var (
	readOpts  = grocksdb.NewDefaultReadOptions()
	writeOpts = grocksdb.NewDefaultWriteOptions()
)

func encodeFrame(b []byte) []byte {
	hdr := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr, uint32(len(b)))
	return append(hdr, b...)
}

func mustJSON(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}

// readEntry assumes values are properly formatted dbEntry JSON.
// Returns value (raw JSON) and true if present and not expired.
// If expired, deletes it and returns miss.
func readEntry(db *grocksdb.DB, key string) (json.RawMessage, bool, error) {
	v, err := db.Get(readOpts, []byte(key))
	if err != nil {
		return nil, false, err
	}
	defer v.Free()
	if !v.Exists() {
		return nil, false, nil
	}

	var e dbEntry
	if err := json.Unmarshal(v.Data(), &e); err != nil {
		// If store is always well-formed this should not happen; propagate error.
		return nil, false, fmt.Errorf("malformed entry for key %q: %w", key, err)
	}

	now := time.Now().UnixNano()
	if e.Expiry != math.MaxInt64 && now > e.Expiry {
		// expired -> delete and return miss
		_ = db.Delete(writeOpts, []byte(key))
		return nil, false, nil
	}

	// return a copy of bytes
	raw := make([]byte, len(e.Value))
	copy(raw, e.Value)
	return json.RawMessage(raw), true, nil
}

// writeEntry writes the wrapper into RocksDB. ttl==0 means infinite expiry.
func writeEntry(db *grocksdb.DB, key string, raw json.RawMessage, ttl time.Duration) error {
	e := dbEntry{Value: raw}
	if ttl == 0 {
		e.Expiry = math.MaxInt64
	} else {
		e.Expiry = time.Now().Add(ttl).UnixNano()
	}
	data, err := json.Marshal(&e)
	if err != nil {
		return err
	}
	return db.Put(writeOpts, []byte(key), data)
}

func deleteEntry(db *grocksdb.DB, key string) error {
	return db.Delete(writeOpts, []byte(key))
}

// listAllEntries returns all non-expired entries. If ttl==0 entries never expire.
func listAllEntries(db *grocksdb.DB) (map[string]interface{}, error) {
	out := map[string]interface{}{}
	it := db.NewIterator(readOpts)
	defer it.Close()
	now := time.Now().UnixNano()

	for it.SeekToFirst(); it.Valid(); it.Next() {
		k := string(it.Key().Data())
		var e dbEntry
		if err := json.Unmarshal(it.Value().Data(), &e); err == nil {
			if e.Expiry == math.MaxInt64 || e.Expiry > now {
				var v interface{}
				_ = json.Unmarshal(e.Value, &v)
				out[k] = v
			}
		} else {
			// With the assumption of well-formed data this branch shouldn't occur.
			// Skip malformed entries to avoid returning garbage.
		}
		it.Key().Free()
		it.Value().Free()
	}
	return out, nil
}

// startCleaner runs when upstream is set AND ttl>0. It deletes expired entries in batches.
func startCleaner(db *grocksdb.DB, interval time.Duration, chunkSize int) {
	t := time.NewTicker(interval)
	go func() {
		for range t.C {
			now := time.Now().UnixNano()
			it := db.NewIterator(readOpts)
			batch := grocksdb.NewWriteBatch()
			count := 0
			for it.SeekToFirst(); it.Valid(); it.Next() {
				var e dbEntry
				if err := json.Unmarshal(it.Value().Data(), &e); err == nil {
					if e.Expiry != math.MaxInt64 && e.Expiry <= now {
						batch.Delete(it.Key().Data())
						count++
					}
				}
				it.Key().Free()
				it.Value().Free()
				if count >= chunkSize {
					_ = db.Write(writeOpts, batch)
					batch.Destroy()
					batch = grocksdb.NewWriteBatch()
					count = 0
				}
			}
			if count > 0 {
				_ = db.Write(writeOpts, batch)
			}
			batch.Destroy()
			it.Close()
		}
	}()
}

// fetchFromUpstream queries the upstream node for a single key.
// Returns raw JSON and true if upstream had key.
func fetchFromUpstream(upstream string, key string) (json.RawMessage, bool, error) {
	if upstream == "" {
		return nil, false, nil
	}
	reqBody := Request{Type: "GET", Keys: []string{key}}
	bodyBytes, _ := json.Marshal(reqBody)
	httpReq, err := http.NewRequest("POST", upstream, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, false, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, false, fmt.Errorf("upstream status %d", resp.StatusCode)
	}
	var r Response
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, false, err
	}
	if r.Type == "ERR" {
		return nil, false, fmt.Errorf("upstream error: %s", r.Error)
	}
	val, ok := r.Data[key]
	if !ok || val == nil {
		return nil, false, nil
	}
	raw, err := json.Marshal(val)
	if err != nil {
		return nil, false, err
	}
	return raw, true, nil
}

// handleRequest uses the datastore semantics: on miss, consult upstream only if UPSTREAM_URL set.
func handleRequest(db *grocksdb.DB, req Request, ttl time.Duration, upstream string) Response {
	switch req.Type {
	case "GET":
		results := make(map[string]interface{})
		for _, key := range req.Keys {
			raw, ok, err := readEntry(db, key)
			if err != nil {
				return Response{Type: "ERR", Error: err.Error()}
			}
			if ok {
				var v interface{}
				_ = json.Unmarshal(raw, &v)
				results[key] = v
				continue
			}

			// miss: only fetch upstream if configured
			if upstream != "" {
				rawUp, found, err := fetchFromUpstream(upstream, key)
				if err != nil {
					return Response{Type: "ERR", Error: err.Error()}
				}
				if found {
					// on write use TTL provided; ttl==0 => infinite
					if err := writeEntry(db, key, rawUp, ttl); err != nil {
						return Response{Type: "ERR", Error: err.Error()}
					}
					var v interface{}
					_ = json.Unmarshal(rawUp, &v)
					results[key] = v
					continue
				}
			}

			results[key] = nil
		}
		return Response{Type: "OK", Data: results}

	case "LIST":
		all, err := listAllEntries(db)
		if err != nil {
			return Response{Type: "ERR", Error: err.Error()}
		}
		return Response{Type: "OK", Data: all}

	case "UPDATE":
		for key, raw := range req.Items {
			if len(raw) == 0 {
				if err := deleteEntry(db, key); err != nil {
					return Response{Type: "ERR", Error: err.Error()}
				}
			} else {
				if err := writeEntry(db, key, raw, ttl); err != nil {
					return Response{Type: "ERR", Error: err.Error()}
				}
			}
		}
		return Response{Type: "OK"}

	default:
		return Response{Type: "ERR", Error: "unknown type"}
	}
}

func writeFrame(conn net.Conn, resp Response) {
	conn.Write(encodeFrame(mustJSON(resp)))
}

func main() {
	upstream := os.Getenv("UPSTREAM_URL") // if set => ephemeral behavior (fetch upstream on miss)
	ttlStr := os.Getenv("CACHE_TTL")      // if empty or "0" => infinite TTL (never expire)
	jIntervalStr := os.Getenv("JANITOR_INTERVAL")
	chunkStr := os.Getenv("JANITOR_CHUNK")
	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "./kvdb"
	}

	var ttl time.Duration
	if ttlStr == "" || ttlStr == "0" {
		ttl = 0 // infinite
	} else if d, err := time.ParseDuration(ttlStr); err == nil {
		ttl = d
	} else {
		fmt.Println("invalid CACHE_TTL, treating as infinite (never expire)")
		ttl = 0
	}

	var janitorInterval time.Duration
	if jIntervalStr == "" {
		janitorInterval = 1 * time.Minute
	} else if d, err := time.ParseDuration(jIntervalStr); err == nil {
		janitorInterval = d
	} else {
		janitorInterval = 1 * time.Minute
	}

	chunkSize := 1000
	if chunkStr != "" {
		var v int
		if _, err := fmt.Sscanf(chunkStr, "%d", &v); err == nil && v > 0 {
			chunkSize = v
		}
	}

	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	db, err := grocksdb.OpenDb(opts, dbPath)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Only start cleaner if we have an upstream AND TTL is finite (>0).
	if upstream != "" && ttl > 0 {
		startCleaner(db, janitorInterval, chunkSize)
	}

	socketPath := "/tmp/kvstore.sock"
	os.Remove(socketPath)
	l, err := net.Listen("unix", socketPath)
	if err != nil {
		panic(err)
	}
	defer l.Close()
	os.Chmod(socketPath, 0660)

	if upstream == "" {
		if ttl == 0 {
			fmt.Printf("Datastore running authoritative (no upstream), TTL=infinite (persist forever)\n")
		} else {
			fmt.Printf("Datastore running authoritative (no upstream), TTL=%s\n", ttl)
		}
	} else {
		if ttl == 0 {
			fmt.Printf("Datastore running ephemeral behavior (UPSTREAM=%s), TTL=infinite\n", upstream)
		} else {
			fmt.Printf("Datastore running ephemeral behavior (UPSTREAM=%s), TTL=%s\n", upstream, ttl)
		}
	}
	fmt.Printf("DB path: %s\n", dbPath)

	// accept unix sockets
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				continue
			}
			go func(c net.Conn) {
				defer c.Close()
				rd := bufio.NewReader(c)
				for {
					hdr := make([]byte, 4)
					if _, err := io.ReadFull(rd, hdr); err != nil {
						return
					}
					payload := make([]byte, binary.BigEndian.Uint32(hdr))
					if _, err := io.ReadFull(rd, payload); err != nil {
						return
					}
					var req Request
					if err := json.Unmarshal(payload, &req); err != nil {
						writeFrame(c, Response{Type: "ERR", Error: err.Error()})
						continue
					}
					resp := handleRequest(db, req, ttl, upstream)
					writeFrame(c, resp)
				}
			}(conn)
		}
	}()

	// http endpoint
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Post("/", func(w http.ResponseWriter, r *http.Request) {
		var req Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		resp := handleRequest(db, req, ttl, upstream)
		if resp.Type == "ERR" {
			http.Error(w, resp.Error, 400)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	fmt.Println("HTTP API listening on :8080")
	http.ListenAndServe(":8080", r)
}