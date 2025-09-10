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
	Type   string          `json:"type"`
	Domain string          `json:"domain,omitempty"`
	Value  json.RawMessage `json:"value,omitempty"`
}

type Response struct {
	Type   string      `json:"type"`
	Error  string      `json:"error,omitempty"`
	Domain string      `json:"domain,omitempty"`
	Value  interface{} `json:"value,omitempty"`
}

// simple pubsub: notify all connected clients on change
var (
	clientsMu sync.Mutex
	clients   = make(map[int]net.Conn)
	nextID    = 0
)

func notifyAll(msg Response) {
	b, _ := json.Marshal(msg)
	frame := encodeFrame(b)
	clientsMu.Lock()
	for id, c := range clients {
		_, err := c.Write(frame)
		if err != nil {
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
	id := 0
	clientsMu.Lock()
	id = nextID
	nextID++
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
		// read 4-byte length
		hdr := make([]byte, 4)
		if _, err := io.ReadFull(rd, hdr); err != nil {
			return
		}
		l := binary.BigEndian.Uint32(hdr)
		payload := make([]byte, l)
		if _, err := io.ReadFull(rd, payload); err != nil {
			return
		}

		var req Request
		if err := json.Unmarshal(payload, &req); err != nil {
			resp := Response{Type: "ERR", Error: err.Error()}
			conn.Write(encodeFrame(mustJSON(resp)))
			continue
		}

		switch req.Type {
		case "GET":
			ro := grocksdb.NewDefaultReadOptions()
			v, err := db.Get(ro, []byte(req.Domain))
			if err != nil {
				conn.Write(encodeFrame(mustJSON(Response{Type: "ERR", Error: err.Error()})))
				continue
			}
			if !v.Exists() {
				conn.Write(encodeFrame(mustJSON(Response{Type: "OK", Domain: req.Domain, Value: nil})))
				continue
			}
			var val interface{}
			if err := json.Unmarshal(v.Data(), &val); err != nil {
				conn.Write(encodeFrame(mustJSON(Response{Type: "ERR", Error: err.Error()})))
				v.Free()
				continue
			}
			v.Free()
			conn.Write(encodeFrame(mustJSON(Response{Type: "OK", Domain: req.Domain, Value: val})))

		case "LIST":
			it := db.NewIterator(grocksdb.NewDefaultReadOptions())
			it.SeekToFirst()
			all := map[string]interface{}{}
			for ; it.Valid(); it.Next() {
				key := string(it.Key().Data())
				var val interface{}
				json.Unmarshal(it.Value().Data(), &val)
				all[key] = val
				it.Key().Free()
				it.Value().Free()
			}
			it.Close()
			conn.Write(encodeFrame(mustJSON(Response{Type: "OK", Value: all})))

		case "UPDATE":
			// req.Domain and req.Value expected
			wo := grocksdb.NewDefaultWriteOptions()
			if err := db.Put(wo, []byte(req.Domain), req.Value); err != nil {
				conn.Write(encodeFrame(mustJSON(Response{Type: "ERR", Error: err.Error()})))
				continue
			}
			// notify all clients of update
			notifyAll(Response{Type: "NOTIFY", Domain: req.Domain, Value: json.RawMessage(req.Value)})
			conn.Write(encodeFrame(mustJSON(Response{Type: "OK"})))

		case "SUBSCRIBE":
			// just keep connection open; notifications are written by notifyAll
			conn.Write(encodeFrame(mustJSON(Response{Type: "OK", Value: "subscribed"})))

		default:
			conn.Write(encodeFrame(mustJSON(Response{Type: "ERR", Error: "unknown type"})))
		}
	}
}

func mustJSON(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}

func main() {
	// open RocksDB
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	db, err := grocksdb.OpenDb(opts, "./confdb")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// start Unix socket listener (for your custom protocol)
	socketPath := "/tmp/confsvc.sock"
	os.Remove(socketPath)
	l, err := net.Listen("unix", socketPath)
	if err != nil {
		panic(err)
	}
	defer l.Close()
	os.Chmod(socketPath, 0660)

	fmt.Println("config server listening on", socketPath)

	// run socket server in a goroutine
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				continue
			}
			go handleConn(conn, db)
		}
	}()

	// setup chi API
	r := chi.NewRouter()
	r.Use(middleware.Logger)

	r.Get("/get/{domain}", func(w http.ResponseWriter, r *http.Request) {
		domain := chi.URLParam(r, "domain")
		ro := grocksdb.NewDefaultReadOptions()
		v, err := db.Get(ro, []byte(domain))
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		defer v.Free()
		if !v.Exists() {
			w.WriteHeader(404)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(v.Data())
	})

	r.Post("/update/{domain}", func(w http.ResponseWriter, r *http.Request) {
		domain := chi.URLParam(r, "domain")
		var val interface{}
		if err := json.NewDecoder(r.Body).Decode(&val); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		data, _ := json.Marshal(val)
		wo := grocksdb.NewDefaultWriteOptions()
		if err := db.Put(wo, []byte(domain), data); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		// also notify socket subscribers
		notifyAll(Response{Type: "NOTIFY", Domain: domain, Value: val})
		w.WriteHeader(204)
	})

	fmt.Println("chi API listening on :8080")
	http.ListenAndServe(":8080", r)
}
