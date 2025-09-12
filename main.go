package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"

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

var (
	readOpts  = grocksdb.NewDefaultReadOptions()
	writeOpts = grocksdb.NewDefaultWriteOptions()
)

func encodeFrame(b []byte) []byte {
	hdr := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr, uint32(len(b)))
	return append(hdr, b...)
}

// Core request handler shared by socket + HTTP
func handleRequest(db *grocksdb.DB, req Request) Response {
	switch req.Type {
	case "BATCH_GET":
		results := make(map[string]interface{})
		for _, key := range req.Keys {
			val, err := readKey(db, key)
			if err != nil {
				return Response{Type: "ERR", Error: err.Error()}
			}
			results[key] = val
		}
		return Response{Type: "OK", Data: results}

	case "LIST":
		all := map[string]interface{}{}
		it := db.NewIterator(readOpts)
		for it.SeekToFirst(); it.Valid(); it.Next() {
			key := string(it.Key().Data())
			var val interface{}
			json.Unmarshal(it.Value().Data(), &val)
			all[key] = val
			it.Key().Free()
			it.Value().Free()
		}
		it.Close()
		return Response{Type: "OK", Data: all}

	case "BATCH_UPDATE":
		batch := grocksdb.NewWriteBatch()
		for key, raw := range req.Items {
			if len(raw) == 0 {
				batch.Delete([]byte(key))
			} else {
				batch.Put([]byte(key), raw)
			}
		}
		if err := db.Write(writeOpts, batch); err != nil {
			return Response{Type: "ERR", Error: err.Error()}
		}
		return Response{Type: "OK"}

	default:
		return Response{Type: "ERR", Error: "unknown type"}
	}
}

func handleConn(conn net.Conn, db *grocksdb.DB) {
	defer conn.Close()
	rd := bufio.NewReader(conn)

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
			writeFrame(conn, Response{Type: "ERR", Error: err.Error()})
			continue
		}

		resp := handleRequest(db, req)
		writeFrame(conn, resp)
	}
}

func readKey(db *grocksdb.DB, key string) (interface{}, error) {
	v, err := db.Get(readOpts, []byte(key))
	if err != nil {
		return nil, err
	}
	defer v.Free()
	if !v.Exists() {
		return nil, nil
	}
	var val interface{}
	if err := json.Unmarshal(v.Data(), &val); err != nil {
		return nil, err
	}
	return val, nil
}

func writeFrame(conn net.Conn, resp Response) {
	conn.Write(encodeFrame(mustJSON(resp)))
}

func mustJSON(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}

func main() {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	db, err := grocksdb.OpenDb(opts, "./kvdb")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	socketPath := "/tmp/kvstore.sock"
	os.Remove(socketPath)
	l, err := net.Listen("unix", socketPath)
	if err != nil {
		panic(err)
	}
	defer l.Close()
	os.Chmod(socketPath, 0660)

	fmt.Println("KV store server listening on", socketPath)

	go func() {
		for {
			if conn, err := l.Accept(); err == nil {
				go handleConn(conn, db)
			}
		}
	}()

	r := chi.NewRouter()
	r.Use(middleware.Logger)

	// Unified HTTP endpoint
	r.Post("/", func(w http.ResponseWriter, r *http.Request) {
		var req Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		resp := handleRequest(db, req)
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