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
	Type  string          `json:"type"`
	Key   string          `json:"key,omitempty"`
	Value json.RawMessage `json:"value,omitempty"`
}

type Response struct {
	Type  string      `json:"type"`
	Error string      `json:"error,omitempty"`
	Key   string      `json:"key,omitempty"`
	Value interface{} `json:"value,omitempty"`
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

		switch req.Type {
		case "GET":
			val, err := readKey(db, req.Key)
			if err != nil {
				writeFrame(conn, Response{Type: "ERR", Error: err.Error()})
				continue
			}
			writeFrame(conn, Response{Type: "OK", Key: req.Key, Value: val})

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
			writeFrame(conn, Response{Type: "OK", Value: all})

		case "UPDATE":
			if len(req.Value) == 0 {
				// Treat empty value as delete
				if err := db.Delete(writeOpts, []byte(req.Key)); err != nil {
					writeFrame(conn, Response{Type: "ERR", Error: err.Error()})
					continue
				}
			} else {
				if err := db.Put(writeOpts, []byte(req.Key), req.Value); err != nil {
					writeFrame(conn, Response{Type: "ERR", Error: err.Error()})
					continue
				}
			}
			writeFrame(conn, Response{Type: "OK"})

		default:
			writeFrame(conn, Response{Type: "ERR", Error: "unknown type"})
		}
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

	r.Get("/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := chi.URLParam(r, "key")
		val, err := readKey(db, key)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		if val == nil {
			w.WriteHeader(404)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(val)
	})

	r.Post("/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := chi.URLParam(r, "key")
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}

		if len(body) == 0 {
			// Empty body → delete key
			if err := db.Delete(writeOpts, []byte(key)); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			w.WriteHeader(204)
			return
		}

		var val interface{}
		if err := json.Unmarshal(body, &val); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if err := db.Put(writeOpts, []byte(key), body); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.WriteHeader(204)
	})

	fmt.Println("HTTP API listening on :8080")
	http.ListenAndServe(":8080", r)
}
