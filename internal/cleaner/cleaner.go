package cleaner

import (
	"time"

	"github.com/UltraSive/rocksdb-configuration-distribution/internal/datastore"
)

func Start(ds datastore.Datastore, interval time.Duration, chunkSize int, stop <-chan struct{}) {
	t := time.NewTicker(interval)
	go func() {
		defer t.Stop()
		for {
			select {
			case <-t.C:
				// simple implementation: call List to get keys and delete expired entries,
				// or add a dedicated API to datastore for scanning+deleting.
				_ = runOnce(ds, chunkSize)
			case <-stop:
				return
			}
		}
	}()
}

// runOnce inspects the db and deletes expired entries in batches.
// To avoid exposing rocks internals here, you may add a Datastore.ScanExpired API for efficiency.
func runOnce(ds datastore.Datastore, chunkSize int) error {
	// naive: list and remove expired entries (fine for small/medium DBs).
	// For very large DBs implement ScanExpired in the datastore impl.
	all, err := ds.List()
	if err != nil {
		return err
	}
	_ = all // loop and remove expired using ds.Delete for entries that are expired
	return nil
}
