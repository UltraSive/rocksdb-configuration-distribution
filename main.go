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
	"sync"

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
	clientsMu sync.Mutex
	clients   = make(map[int]net.Conn)
	nextID    = 0

	readOpts  = grocksdb.NewDefaultReadOptions()
	writeOpts = grocksdb.NewDefaultWriteOptions()
)

func notifyAll(msg Response) {
	b, _ := json.Marshal(msg)
	frame := encodeFrame(b)
	clientsMu.Lock()
	for id, c := range clients {
		if _, err := c.Write(frame); err != nil {
			c.Close()
			delete(clients, id)
		}
	}
	clientsMu.Unlock()
}

func encodeFrame(b []byte) []byte {
	hdr := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr, uint32(len(b)))
	return append(hdr, b...)
}

func handleConn(conn net.Conn, db *grocksdb.DB) {
	id := nextID
	nextID++
	clientsMu.Lock()
	clients[id] = conn
	clientsMu.Unlock()
	defer func() {
		conn.Close()
		clientsMu.Lock()
		delete(clients, id)
		clientsMu.Unlock()
	}()

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
			if err := db.Put(writeOpts, []byte(req.Key), req.Value); err != nil {
				writeFrame(conn, Response{Type: "ERR", Error: err.Error()})
				continue
			}
			notifyAll(Response{Type: "NOTIFY", Key: req.Key, Value: json.RawMessage(req.Value)})
			writeFrame(conn, Response{Type: "OK"})

		case "SUBSCRIBE":
			writeFrame(conn, Response{Type: "OK", Value: "subscribed"})

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
		var val interface{}
		if err := json.NewDecoder(r.Body).Decode(&val); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		data, _ := json.Marshal(val)
		if err := db.Put(writeOpts, []byte(key), data); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		notifyAll(Response{Type: "NOTIFY", Key: key, Value: val})
		w.WriteHeader(204)
	})

	fmt.Println("HTTP API listening on :8080")
	http.ListenAndServe(":8080", r)
}
