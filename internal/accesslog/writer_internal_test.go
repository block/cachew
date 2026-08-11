package accesslog

import (
	"compress/gzip"
	"encoding/json"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
)

func TestEncodeBatch(t *testing.T) {
	events := []Event{
		{Timestamp: time.Unix(1, 0).UTC(), Method: "GET", Path: "/a", Status: 200},
		{Timestamp: time.Unix(2, 0).UTC(), Method: "POST", Path: "/b", Status: 403},
	}

	buf, suffix, contentType, err := encodeBatch(events, CompressionNone)
	assert.NoError(t, err)
	assert.Equal(t, ".jsonl", suffix)
	assert.Equal(t, "application/x-ndjson", contentType)
	var decoded []Event
	dec := json.NewDecoder(buf)
	for dec.More() {
		var event Event
		assert.NoError(t, dec.Decode(&event))
		decoded = append(decoded, event)
	}
	assert.Equal(t, events, decoded)

	buf, suffix, contentType, err = encodeBatch(events, CompressionGzip)
	assert.NoError(t, err)
	assert.Equal(t, ".jsonl.gz", suffix)
	assert.Equal(t, "application/gzip", contentType)
	gz, err := gzip.NewReader(buf)
	assert.NoError(t, err)
	decoded = nil
	dec = json.NewDecoder(gz)
	for dec.More() {
		var event Event
		assert.NoError(t, dec.Decode(&event))
		decoded = append(decoded, event)
	}
	assert.NoError(t, gz.Close())
	assert.Equal(t, events, decoded)
}
