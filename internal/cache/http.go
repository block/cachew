package cache

import (
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/textproto"
	"os"

	"github.com/alecthomas/errors"
)

type HTTPError struct {
	status int
	err    error
}

func (h HTTPError) Error() string   { return fmt.Sprintf("%d: %s", h.status, h.err) }
func (h HTTPError) Unwrap() error   { return h.err }
func (h HTTPError) StatusCode() int { return h.status }

func HTTPErrorf(status int, format string, args ...any) error {
	return HTTPError{
		status: status,
		err:    errors.Errorf(format, args...),
	}
}

// Fetch retrieves a response from cache or fetches from the request URL and caches it.
// The response is streamed without buffering. Returns HTTPError for semantic errors.
// The caller must close the response body.
func Fetch(client *http.Client, r *http.Request, c Cache) (*http.Response, error) {
	url := r.URL.String()
	key := NewKey(url)

	cr, headers, err := c.Open(r.Context(), key)
	if err == nil {
		return &http.Response{
			Status:        "200 OK",
			StatusCode:    http.StatusOK,
			Proto:         "HTTP/1.1",
			ProtoMajor:    1,
			ProtoMinor:    1,
			Header:        http.Header(headers),
			Body:          cr,
			ContentLength: -1,
			Request:       r,
		}, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, HTTPErrorf(http.StatusInternalServerError, "failed to open cache: %w", err)
	}

	resp, err := client.Do(r) //nolint:bodyclose // Body is returned to caller
	if err != nil {
		return nil, HTTPErrorf(http.StatusBadGateway, "failed to fetch: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return resp, nil
	}

	responseHeaders := textproto.MIMEHeader(maps.Clone(resp.Header))
	cw, err := c.Create(r.Context(), key, responseHeaders, 0)
	if err != nil {
		_ = resp.Body.Close()
		return nil, HTTPErrorf(http.StatusInternalServerError, "failed to create cache entry: %w", err)
	}

	originalBody := resp.Body
	pr, pw := io.Pipe()
	go func() {
		mw := io.MultiWriter(pw, cw)
		_, copyErr := io.Copy(mw, originalBody)
		closeErr := errors.Join(cw.Close(), originalBody.Close())
		pw.CloseWithError(errors.Join(copyErr, closeErr))
	}()

	resp.Body = pr
	return resp, nil
}
