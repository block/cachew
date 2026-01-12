// Package httputil contains utilities for HTTP clients and servers.
package httputil

import (
	"fmt"
	"net/http"

	"github.com/alecthomas/errors"

	"github.com/block/sfptc/internal/logging"
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
}

func (h HTTPError) Error() string { return fmt.Sprintf("%d: %s", h.status, h.err) }
func (h HTTPError) Unwrap() error { return h.err }

// WriteHTTP writes this error as an HTTP response.
func (h HTTPError) WriteHTTP(w http.ResponseWriter, r *http.Request) {
	ErrorResponse(w, r, h.status, h.err.Error())
}

func Errorf(status int, format string, args ...any) error {
	return HTTPError{
		status: status,
		err:    errors.Errorf(format, args...),
	}
}
