// Package httputil contains utilities for HTTP clients and servers.
package httputil

import (
	"fmt"
	"maps"
	"net/http"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/logging"
)

// ErrorResponse creates an error response with the given code and format, and also logs a message.
func ErrorResponse(w http.ResponseWriter, r *http.Request, status int, msg string, args ...any) {
	logger := logging.FromContext(r.Context()).With("url", r.URL, "status", status)
	logger.ErrorContext(r.Context(), msg, args...)
	http.Error(w, msg, status)
}

// HTTPResponder is an error that knows how to write itself as an HTTP response.
type HTTPResponder interface {
	error
	WriteHTTP(http.ResponseWriter, *http.Request)
}

type HTTPError struct {
	status int
	err    error
	header http.Header
}

func (h HTTPError) Error() string { return fmt.Sprintf("%d: %s", h.status, h.err) }
func (h HTTPError) Unwrap() error { return h.err }

// WriteHTTP writes this error as an HTTP response.
func (h HTTPError) WriteHTTP(w http.ResponseWriter, r *http.Request) {
	maps.Copy(w.Header(), h.header)
	ErrorResponse(w, r, h.status, h.err.Error())
}

func Errorf(status int, format string, args ...any) error {
	return HTTPError{
		status: status,
		err:    errors.Errorf(format, args...),
	}
}

// UpstreamErrorf is [Errorf] with the status of resp. For a 429 it keeps
// Retry-After and marks the response no-store.
func UpstreamErrorf(resp *http.Response, format string, args ...any) error {
	err := HTTPError{status: resp.StatusCode, err: errors.Errorf(format, args...)}
	if resp.StatusCode == http.StatusTooManyRequests {
		err.header = http.Header{"Cache-Control": {"no-store"}}
		if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
			err.header.Set("Retry-After", retryAfter)
		}
	}
	return err
}
