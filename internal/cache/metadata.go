package cache

import (
	"maps"
	"net/http"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/httputil"
)

// updatedHeaders returns stored with update's headers replacing their stored
// values, after checking the update against the stored ETag. Backends share it
// so UpdateMetadata has identical semantics everywhere.
func updatedHeaders(stored, update http.Header, opts []Option) (http.Header, error) {
	if update.Get(ETagKey) != "" {
		return nil, errors.New("UpdateMetadata cannot change the ETag")
	}
	if err := NewRequestOptions(opts...).Check(stored.Get(ETagKey)); err != nil {
		return nil, errors.WithStack(err)
	}
	merged := maps.Clone(stored)
	if merged == nil {
		merged = make(http.Header)
	}
	maps.Copy(merged, httputil.FilterHeaders(update, httputil.TransportHeaders...))
	return merged, nil
}
