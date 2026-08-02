package gitcredential_test

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/gitcredential"
)

func TestDecodeRequest(t *testing.T) {
	request, err := gitcredential.DecodeRequest(bytes.NewBufferString(
		"{\"version\":1,\"remote_url\":\"HTTPS://EXAMPLE.COM:443/org/repo.git/\"}\n"))
	assert.NoError(t, err)
	assert.Equal(t, gitcredential.ProtocolVersion, request.Version)
	assert.Equal(t, "https://example.com/org/repo", request.RemoteURL)
}

func TestDecodeRequestRejectsUnknownFields(t *testing.T) {
	_, err := gitcredential.DecodeRequest(bytes.NewBufferString(
		"{\"version\":1,\"remote_url\":\"https://example.com/org/repo\",\"token\":\"secret\"}\n"))
	assert.Error(t, err)
}

func TestEncodeResponse(t *testing.T) {
	output := &bytes.Buffer{}
	expiresAt := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	err := gitcredential.EncodeResponse(output, gitcredential.Response{
		Version:       gitcredential.ProtocolVersion,
		Authorization: "Bearer token",
		ExpiresAt:     expiresAt,
	})
	assert.NoError(t, err)
	var response gitcredential.Response
	assert.NoError(t, json.Unmarshal(output.Bytes(), &response))
	assert.Equal(t, "Bearer token", response.Authorization)
	assert.True(t, expiresAt.Equal(response.ExpiresAt))
}
