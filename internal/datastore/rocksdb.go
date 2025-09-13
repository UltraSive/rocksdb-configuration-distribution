package datastore

import (
	"encoding/json"
	"math"
	"time"

	"github.com/linxGnu/grocksdb"
)

type RocksDB struct {
	db        *grocksdb.DB
	readOpts  *grocksdb.ReadOptions
	writeOpts *grocksdb.WriteOptions
}

func NewRocksDB(path string) (*RocksDB, error) {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	db, err := grocksdb.OpenDb(opts, path)
	if err != nil {
		return nil, err
	}
	return &RocksDB{
		db:        db,
		readOpts:  grocksdb.NewDefaultReadOptions(),
		writeOpts: grocksdb.NewDefaultWriteOptions(),
	}, nil
}

func (r *RocksDB) Get(key string) (json.RawMessage, bool, error) {
	v, err := r.db.Get(r.readOpts, []byte(key))
	if err != nil {
		return nil, false, err
	}
	defer v.Free()
	if !v.Exists() {
		return nil, false, nil
	}
	var e DBEntry
	if err := json.Unmarshal(v.Data(), &e); err != nil {
		return nil, false, err
	}
	now := time.Now().UnixNano()
	if e.Expiry != math.MaxInt64 && now > e.Expiry {
		_ = r.db.Delete(r.writeOpts, []byte(key))
		return nil, false, nil
	}
	raw := make([]byte, len(e.Value))
	copy(raw, e.Value)
	return json.RawMessage(raw), true, nil
}

func (r *RocksDB) Put(key string, value json.RawMessage, ttl time.Duration) error {
	e := DBEntry{Value: value}
	if ttl == 0 {
		e.Expiry = math.MaxInt64
	} else {
		e.Expiry = time.Now().Add(ttl).UnixNano()
	}
	data, _ := json.Marshal(&e)
	return r.db.Put(r.writeOpts, []byte(key), data)
}

func (r *RocksDB) Delete(key string) error {
	return r.db.Delete(r.writeOpts, []byte(key))
}

func (r *RocksDB) List() (map[string]interface{}, error) {
	out := make(map[string]interface{})
	it := r.db.NewIterator(r.readOpts)
	defer it.Close()
	now := time.Now().UnixNano()
	for it.SeekToFirst(); it.Valid(); it.Next() {
		var e DBEntry
		if err := json.Unmarshal(it.Value().Data(), &e); err == nil {
			if e.Expiry == math.MaxInt64 || e.Expiry > now {
				var v interface{}
				_ = json.Unmarshal(e.Value, &v)
				out[string(it.Key().Data())] = v
			}
		}
		it.Key().Free()
		it.Value().Free()
	}
	return out, nil
}

func (r *RocksDB) Close() error {
	r.readOpts.Destroy()
	r.writeOpts.Destroy()
	r.db.Close()
	return nil
}
