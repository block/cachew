package metadatadb_test

import (
	"testing"
	"time"

	"github.com/block/cachew/internal/metadatadb"
	"github.com/block/cachew/internal/metadatadb/metadatadbtest"
)

func TestMemoryBackend(t *testing.T) {
	metadatadbtest.Suite(t, func(t *testing.T) metadatadb.Backend {
		t.Helper()
		return metadatadb.NewMemoryBackend()
	})
}

func TestMemoryBackendSoak(t *testing.T) {
	metadatadbtest.Soak(t, metadatadb.NewMemoryBackend(), metadatadbtest.SoakConfig{
		Duration:    5 * time.Second,
		Concurrency: 4,
		NumKeys:     20,
	})
}
