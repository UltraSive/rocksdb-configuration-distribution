package handler

import (
	"encoding/json"
	"time"

	"github.com/UltraSive/rocksdb-configuration-distribution/internal/datastore"
	"github.com/UltraSive/rocksdb-configuration-distribution/internal/upstream"
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

type Handler struct {
	DB       datastore.Datastore
	Upstream *upstream.Client // nil if none
	TTL      time.Duration   // 0 == infinite
}

func New(db datastore.Datastore, up *upstream.Client, ttl time.Duration) *Handler {
	return &Handler{DB: db, Upstream: up, TTL: ttl}
}

func (h *Handler) Serve(req Request) Response {
	switch req.Type {
	case "GET":
		res := make(map[string]interface{})
		for _, k := range req.Keys {
			raw, ok, err := h.DB.Get(k)
			if err != nil {
				return Response{Type: "ERR", Error: err.Error()}
			}
			if ok {
				var v interface{}
				_ = json.Unmarshal(raw, &v)
				res[k] = v
				continue
			}
			// miss -> ask upstream if configured
			if h.Upstream != nil {
				rawUp, found, err := h.Upstream.Fetch(k)
				if err != nil {
					return Response{Type: "ERR", Error: err.Error()}
				}
				if found {
					_ = h.DB.Put(k, rawUp, h.TTL)
					var v interface{}
					_ = json.Unmarshal(rawUp, &v)
					res[k] = v
					continue
				}
			}
			res[k] = nil
		}
		return Response{Type: "OK", Data: res}

	case "LIST":
		all, err := h.DB.List()
		if err != nil {
			return Response{Type: "ERR", Error: err.Error()}
		}
		return Response{Type: "OK", Data: all}

	case "UPDATE":
		for k, raw := range req.Items {
			if len(raw) == 0 {
				if err := h.DB.Delete(k); err != nil {
					return Response{Type: "ERR", Error: err.Error()}
				}
			} else {
				if err := h.DB.Put(k, raw, h.TTL); err != nil {
					return Response{Type: "ERR", Error: err.Error()}
				}
			}
		}
		return Response{Type: "OK"}

	default:
		return Response{Type: "ERR", Error: "unknown type"}
	}
}
