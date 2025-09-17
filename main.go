package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
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

func encodeFrame(b []byte) []byte {
	hdr := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr, uint32(len(b)))
	return append(hdr, b...)
}

func mustJSON(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}

// Transaction-aware helpers

func readEntryTxn(txn *badger.Txn, key string) (json.RawMessage, bool, error) {
	item, err := txn.Get([]byte(key))
	if err == badger.ErrKeyNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	val, err := item.ValueCopy(nil)
	if err != nil {
		return nil, false, err
	}
	return val, true, nil
}

func writeEntryTxn(txn *badger.Txn, key string, raw json.RawMessage, ttl time.Duration) error {
	e := badger.NewEntry([]byte(key), raw)
	if ttl > 0 {
		e.WithTTL(ttl)
	}
	return txn.SetEntry(e)
}

func deleteEntryTxn(txn *badger.Txn, key string) error {
	return txn.Delete([]byte(key))
}

// Single Key Helpers

func readEntry(db *badger.DB, key string) (json.RawMessage, bool, error) {
	var result json.RawMessage
	var found bool
	err := db.View(func(txn *badger.Txn) error {
		val, ok, err := readEntryTxn(txn, key)
		if err != nil {
			return err
		}
		if ok {
			result = val
			found = true
		}
		return nil
	})
	return result, found, err
}

func writeEntry(db *badger.DB, key string, raw json.RawMessage, ttl time.Duration) error {
	return db.Update(func(txn *badger.Txn) error {
		return writeEntryTxn(txn, key, raw, ttl)
	})
}

func deleteEntry(db *badger.DB, key string) error {
	return db.Update(func(txn *badger.Txn) error {
		return deleteEntryTxn(txn, key)
	})
}

// listAllEntries simply iterates Badger; expired keys are skipped automatically
func listAllEntries(db *badger.DB) (map[string]interface{}, error) {
	out := make(map[string]interface{})
	err := db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := string(item.Key())
			val, err := item.ValueCopy(nil)
			if err != nil {
				continue
			}
			var v interface{}
			if err := json.Unmarshal(val, &v); err == nil {
				out[k] = v
			}
		}
		return nil
	})
	return out, err
}

// background GC using Badger's value log GC
func startBadgerGC(db *badger.DB, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
		again:
			err := db.RunValueLogGC(0.7)
			if err == nil {
				// successfully cleaned, try again immediately
				goto again
			}
			// otherwise, no file was garbage-collected this time
		}
	}()
}

// fetchFromUpstream queries the upstream node for a single key.
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

func handleRequest(db *badger.DB, req Request, ttl time.Duration, upstream string) Response {
	switch req.Type {
	case "GET":
		results := make(map[string]interface{})
		err := db.View(func(txn *badger.Txn) error {
			for _, key := range req.Keys {
				raw, ok, err := readEntryTxn(txn, key)
				if err != nil {
					return err
				}
				if ok {
					var v interface{}
					_ = json.Unmarshal(raw, &v)
					results[key] = v
				} else {
					results[key] = nil
				}
			}
			return nil
		})
		if err != nil {
			return Response{Type: "ERR", Error: err.Error()}
		}
		return Response{Type: "OK", Data: results}

	case "LIST":
		all, err := listAllEntries(db)
		if err != nil {
			return Response{Type: "ERR", Error: err.Error()}
		}
		return Response{Type: "OK", Data: all}

	case "UPDATE":
		err := db.Update(func(txn *badger.Txn) error {
			for key, raw := range req.Items {
				if len(raw) == 0 {
					if err := deleteEntryTxn(txn, key); err != nil {
						return err
					}
				} else {
					if err := writeEntryTxn(txn, key, raw, ttl); err != nil {
						return err
					}
				}
			}
			return nil
		})
		if err != nil {
			return Response{Type: "ERR", Error: err.Error()}
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
	upstream := os.Getenv("UPSTREAM_URL")
	ttlStr := os.Getenv("CACHE_TTL")
	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "./kvdb"
	}

	var ttl time.Duration
	if ttlStr == "" || ttlStr == "0" {
		ttl = 0
	} else if d, err := time.ParseDuration(ttlStr); err == nil {
		ttl = d
	} else {
		fmt.Println("invalid CACHE_TTL, treating as infinite (never expire)")
		ttl = 0
	}

	opts := badger.DefaultOptions(dbPath).WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Always run GC in the background
	startBadgerGC(db, 5*time.Minute)

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