package client

import "github.com/alecthomas/errors"

// ErrStatsUnavailable is returned when a cache backend cannot provide statistics.
var ErrStatsUnavailable = errors.New("stats unavailable")

// Stats contains health and usage statistics for a cache.
type Stats struct {
	// Objects is the number of objects currently in the cache.
	Objects int64 `json:"objects"`
	// Size is the total size of all objects in the cache in bytes.
	Size int64 `json:"size"`
	// Capacity is the maximum size of the cache in bytes (0 if unlimited).
	Capacity int64 `json:"capacity"`
}
