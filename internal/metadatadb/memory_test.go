package metadatadb_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/metadatadb"
	"github.com/block/cachew/internal/metadatadb/metadatadbtest"
)

func TestMemoryBackend(t *testing.T) {
	metadatadbtest.Suite(t, func(t *testing.T, n int) []metadatadb.Backend {
		t.Helper()
		backend := metadatadb.NewMemoryBackend()
		backends := make([]metadatadb.Backend, n)
		for i := range backends {
			backends[i] = backend
		}
		return backends
	})
}

func TestMemoryBackendConcurrentQueryOfMissingNamespaces(t *testing.T) {
	backend := metadatadb.NewMemoryBackend()
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			var value string
			err := backend.Query(t.Context(), fmt.Sprintf("ns-%d", i), metadatadb.ScalarGet{Key: "key"}, &value)
			assert.NoError(t, err)
			assert.Equal(t, "", value)
		})
	}
	wg.Wait()
}
