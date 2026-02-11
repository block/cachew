package cache

import (
	"io"
	"maps"
	"net/http"
	"os"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/internal/httputil"
)

// Fetch retrieves a response from cache or fetches from the request URL and caches it.
// The response is streamed without buffering. Returns HTTPError for semantic errors.
// The caller must close the response body.
func Fetch(client *http.Client, r *http.Request, c Cache) (*http.Response, error) {
	url := r.URL.String()
	key := NewKey(url)

	cr, headers, err := c.Open(r.Context(), "", key)
	if err == nil {
		return &http.Response{
			Status:        "200 OK",
			StatusCode:    http.StatusOK,
			Proto:         "HTTP/1.1",
			ProtoMajor:    1,
			ProtoMinor:    1,
			Header:        headers,
			Body:          cr,
			ContentLength: -1,
			Request:       r,
		}, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, httputil.Errorf(http.StatusInternalServerError, "failed to open cache: %w", err)
	}

	return FetchDirect(client, r, c, key)
}

// FetchDirect fetches and caches the given URL without checking the cache first.
// The response is streamed without buffering. Returns HTTPError for semantic errors.
// The caller must close the response body.
func FetchDirect(client *http.Client, r *http.Request, c Cache, key Key) (*http.Response, error) {
	resp, err := client.Do(r) //nolint:bodyclose // Body is returned to caller
	if err != nil {
		return nil, httputil.Errorf(http.StatusBadGateway, "failed to fetch: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return resp, nil
	}

	responseHeaders := maps.Clone(resp.Header)
	cw, err := c.Create(r.Context(), "", key, responseHeaders, 0)
	if err != nil {
		_ = resp.Body.Close()
		return nil, httputil.Errorf(http.StatusInternalServerError, "failed to create cache entry: %w", err)
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
