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

// readEntry reads a key from rocksdb and returns raw json value if present & valid.
// If replicaMode == true, treat expiry == MaxInt64 as never-expire and accept legacy raw values.
// If running in ephemeral mode (upstream set) and stored value is legacy raw (no wrapper),
// this will wrap it with ttl (or infinite expiry if ttl==0) and write it back for consistent expiry handling.
func readEntry(db *grocksdb.DB, key string, ttl time.Duration, replicaMode bool) (json.RawMessage, bool, error) {
	v, err := db.Get(readOpts, []byte(key))
	if err != nil {
		return nil, false, err
	}
	defer v.Free()
	if !v.Exists() {
		return nil, false, nil
	}

	now := time.Now().UnixNano()

	// Try to unmarshal wrapper first
	var w dbEntry
	if err := json.Unmarshal(v.Data(), &w); err == nil {
		// wrapper parsed
		if w.Expiry != math.MaxInt64 && now > w.Expiry {
			// expired -> delete and miss
			_ = db.Delete(writeOpts, []byte(key))
			return nil, false, nil
		}
		raw := make([]byte, len(w.Value))
		copy(raw, w.Value)
		return json.RawMessage(raw), true, nil
	}

	// not a wrapper (legacy raw bytes)
	rawBytes := make([]byte, len(v.Data()))
	copy(rawBytes, v.Data())

	if replicaMode {
		// in replica (authoritative) mode, legacy raw is authoritative; return as-is
		return json.RawMessage(rawBytes), true, nil
	}

	// ephemeral mode: wrap legacy raw with ttl (or infinite expiry if ttl==0), write back, and return
	if err := writeEntry(db, key, json.RawMessage(rawBytes), ttl, false); err != nil {
		// write failure -> still return the raw value (best-effort)
		return json.RawMessage(rawBytes), true, nil
	}
	return json.RawMessage(rawBytes), true, nil
}

// writeEntry writes a value into RocksDB. If replicaMode==true OR ttl==0 we store expiry=maxint (never expire),
// otherwise we set expiry to now+ttl.
func writeEntry(db *grocksdb.DB, key string, raw json.RawMessage, ttl time.Duration, replicaMode bool) error {
	w := dbEntry{
		Value: raw,
	}
	if replicaMode || ttl == 0 {
		w.Expiry = math.MaxInt64
	} else {
		w.Expiry = time.Now().Add(ttl).UnixNano()
	}
	data, err := json.Marshal(&w)
	if err != nil {
		return err
	}
	return db.Put(writeOpts, []byte(key), data)
}

func deleteEntry(db *grocksdb.DB, key string) error {
	return db.Delete(writeOpts, []byte(key))
}

// listAllEntries returns a map of keys->values for entries that are currently valid.
// In replicaMode it returns everything (including legacy raw values).
func listAllEntries(db *grocksdb.DB, replicaMode bool) (map[string]interface{}, error) {
	ret := map[string]interface{}{}
	it := db.NewIterator(readOpts)
	defer it.Close()
	now := time.Now().UnixNano()

	for it.SeekToFirst(); it.Valid(); it.Next() {
		k := string(it.Key().Data())
		valBytes := it.Value().Data()

		var v interface{}
		// try wrapper
		var w dbEntry
		if err := json.Unmarshal(valBytes, &w); err == nil {
			// wrapper parsed
			if replicaMode || w.Expiry == math.MaxInt64 || w.Expiry > now {
				_ = json.Unmarshal(w.Value, &v)
				ret[k] = v
			}
		} else {
			// legacy raw bytes
			if replicaMode {
				_ = json.Unmarshal(valBytes, &v)
				ret[k] = v
			}
			// in ephemeral mode we skip legacy/malformed entries until read (read will wrap them)
		}

		it.Key().Free()
		it.Value().Free()
	}
	return ret, nil
}

// startCleaner runs only when upstream is set AND ttl>0; it scans and deletes expired entries in chunks
func startCleaner(db *grocksdb.DB, interval time.Duration, chunkSize int) {
	t := time.NewTicker(interval)
	go func() {
		for range t.C {
			now := time.Now().UnixNano()
			it := db.NewIterator(readOpts)
			batch := grocksdb.NewWriteBatch()
			count := 0
			for it.SeekToFirst(); it.Valid(); it.Next() {
				var w dbEntry
				if err := json.Unmarshal(it.Value().Data(), &w); err == nil {
					if w.Expiry != math.MaxInt64 && w.Expiry <= now {
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

// handleRequest handles GET/LIST/UPDATE using the DB + mode toggles.
func handleRequest(db *grocksdb.DB, req Request, ttl time.Duration, upstream string) Response {
	replicaMode := upstream == ""

	switch req.Type {
	case "GET":
		results := make(map[string]interface{})
		for _, key := range req.Keys {
			raw, ok, err := readEntry(db, key, ttl, replicaMode)
			if err != nil {
				return Response{Type: "ERR", Error: err.Error()}
			}
			if ok {
				var v interface{}
				_ = json.Unmarshal(raw, &v)
				results[key] = v
				continue
			}

			// miss: only fetch upstream if upstream is configured (ephemeral mode)
			if !replicaMode {
				rawUp, found, err := fetchFromUpstream(upstream, key)
				if err != nil {
					return Response{Type: "ERR", Error: err.Error()}
				}
				if found {
					if err := writeEntry(db, key, rawUp, ttl, false); err != nil {
						return Response{Type: "ERR", Error: err.Error()}
					}
					var v interface{}
					_ = json.Unmarshal(rawUp, &v)
					results[key] = v
					continue
				}
			}

			// not found
			results[key] = nil
		}
		return Response{Type: "OK", Data: results}

	case "LIST":
		replicaMode := upstream == ""
		all, err := listAllEntries(db, replicaMode)
		if err != nil {
			return Response{Type: "ERR", Error: err.Error()}
		}
		return Response{Type: "OK", Data: all}

	case "UPDATE":
		// Update acts as "persist/populate" or delete (if value empty)
		replicaMode := upstream == ""
		for key, raw := range req.Items {
			if len(raw) == 0 {
				if err := deleteEntry(db, key); err != nil {
					return Response{Type: "ERR", Error: err.Error()}
				}
			} else {
				if err := writeEntry(db, key, raw, ttl, replicaMode); err != nil {
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
	// Configuration from env
	upstream := os.Getenv("UPSTREAM_URL") // if set => ephemeral mode, otherwise replica mode
	ttlStr := os.Getenv("CACHE_TTL")      // if empty => infinite TTL (never expire)
	jIntervalStr := os.Getenv("JANITOR_INTERVAL")
	chunkStr := os.Getenv("JANITOR_CHUNK")
	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "./kvdb"
	}

	var ttl time.Duration
	if ttlStr == "" {
		// empty TTL means "never expire" (infinite)
		ttl = 0
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

	replicaMode := upstream == ""

	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	db, err := grocksdb.OpenDb(opts, dbPath)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Start cleaner when ttl > 0 (i.e. a finite TTL was configured).
	if ttl > 0 {
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

	if replicaMode {
		fmt.Printf("RocksDB instance running in replica (authoritative) mode\n")
	} else {
		if ttl == 0 {
			fmt.Printf("RocksDB instance running in ephemeral mode (UPSTREAM=%s, TTL=infinite)\n", upstream)
		} else {
			fmt.Printf("RocksDB instance running in ephemeral mode (UPSTREAM=%s, TTL=%s)\n", upstream, ttl)
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