package datastore

import (
	"encoding/json"
	"time"
)

// DBEntry matches your on-disk wrapper
type DBEntry struct {
	Expiry int64           `json:"expiry"`
	Value  json.RawMessage `json:"value"`
}

// Datastore defines the minimal operations we need.
type Datastore interface {
	Get(key string) (json.RawMessage, bool, error)
	Put(key string, value json.RawMessage, ttl time.Duration) error
	Delete(key string) error
	List() (map[string]interface{}, error)
	Close() error
}