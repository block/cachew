package cache

import (
	"net/http"
	"time"
)

// expiresAtHeader stores when an explicit TTL ends, so copies into other tiers never outlive the writer's TTL.
//
// Tiered owns it: callers can't set it, and Tiered removes it from the headers it returns.
const expiresAtHeader = "X-Cachew-Expires-At"

// withExpiresAt returns headers for Create, replacing any caller-supplied expiry with the end of an explicit ttl.
func withExpiresAt(headers http.Header, ttl time.Duration, now time.Time) http.Header { //nolint:privatedoc // explains why
	stamped := headers.Clone()
	if stamped == nil {
		stamped = http.Header{}
	}
	stamped.Del(expiresAtHeader)
	if ttl > 0 {
		stamped.Set(expiresAtHeader, now.Add(ttl).UTC().Format(time.RFC3339Nano))
	}
	return stamped
}

// withUpdatedExpiresAt returns headers for UpdateMetadata, which merges headers and so can't remove one.
//
// An empty value clears the expiry when ttl is zero, as the new lifetime is each tier's maximum.
func withUpdatedExpiresAt(headers http.Header, ttl time.Duration, now time.Time) http.Header { //nolint:privatedoc // explains why
	update := withExpiresAt(headers, ttl, now)
	if ttl <= 0 {
		update.Set(expiresAtHeader, "")
	}
	return update
}

// withoutExpiresAt hides the expiry from callers, as it is bookkeeping between tiers rather than object metadata.
func withoutExpiresAt(headers http.Header) http.Header { //nolint:privatedoc // explains why
	if headers.Values(expiresAtHeader) == nil {
		return headers
	}
	headers = headers.Clone()
	headers.Del(expiresAtHeader)
	return headers
}

// backfillTTL returns the TTL for copying an entry into another tier, and false if the writer's expiry has passed.
//
// Zero, the destination's maximum TTL, applies to entries written without an explicit TTL.
func backfillTTL(headers http.Header, now time.Time) (time.Duration, bool) { //nolint:privatedoc // explains why
	expiresAt, err := time.Parse(time.RFC3339Nano, headers.Get(expiresAtHeader))
	if err != nil {
		return 0, true
	}
	ttl := expiresAt.Sub(now)
	return ttl, ttl > 0
}
