package gitcredential

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"time"

	"github.com/alecthomas/errors"
)

// ProtocolVersion is the current external credential command protocol version.
const ProtocolVersion = 1

// Request is the request Cachew sends to an external credential command.
type Request struct {
	Version   int    `json:"version"`
	RemoteURL string `json:"remote_url"`
}

// Response is the response returned by an external credential command.
type Response struct {
	Version       int       `json:"version"`
	Authorization string    `json:"authorization"`
	ExpiresAt     time.Time `json:"expires_at"`
}

// DecodeRequest reads, validates, and canonicalizes one credential command request.
func DecodeRequest(reader io.Reader) (Request, error) {
	data, err := io.ReadAll(io.LimitReader(reader, maxCommandOutput+1))
	if err != nil {
		return Request{}, errors.Wrap(err, "read credential request")
	}
	if len(data) > maxCommandOutput {
		return Request{}, errors.Errorf("credential request exceeds %d bytes", maxCommandOutput)
	}
	var request Request
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return Request{}, errors.Wrap(err, "decode credential request")
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return Request{}, errors.Wrap(err, "decode credential request")
	}
	if request.Version != ProtocolVersion {
		return Request{}, errors.Errorf("unsupported protocol version %d", request.Version)
	}
	canonical, err := NormalizeRepositoryURL(request.RemoteURL)
	if err != nil {
		return Request{}, errors.Wrap(err, "invalid remote URL")
	}
	request.RemoteURL = canonical
	return request, nil
}

// EncodeResponse validates and writes one credential command response.
func EncodeResponse(writer io.Writer, response Response) error {
	if response.Version != ProtocolVersion {
		return errors.Errorf("unsupported protocol version %d", response.Version)
	}
	if response.Authorization == "" || strings.TrimSpace(response.Authorization) != response.Authorization ||
		strings.ContainsAny(response.Authorization, "\r\n\x00") {
		return errors.New("invalid authorization value")
	}
	if response.ExpiresAt.IsZero() || !response.ExpiresAt.After(time.Now()) {
		return errors.New("expired credential")
	}
	if err := json.NewEncoder(writer).Encode(response); err != nil {
		return errors.Wrap(err, "encode credential response")
	}
	return nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return errors.WithStack(err)
	}
	return nil
}
