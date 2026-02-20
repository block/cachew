package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"time"

	"github.com/alecthomas/errors"
)

// Remote implements Cache as a client for the remote cache server.
type Remote struct {
	baseURL string
	client  *http.Client
}

var _ Cache = (*Remote)(nil)

// NewRemote creates a new remote cache client.
func NewRemote(baseURL string) *Remote {
	transport := http.DefaultTransport.(*http.Transport).Clone() //nolint:errcheck
	transport.MaxIdleConns = 100
	transport.MaxIdleConnsPerHost = 100

	return &Remote{
		baseURL: baseURL + "/api/v1",
		client:  &http.Client{Transport: transport},
	}
}

func (c *Remote) String() string { return "remote:" + c.baseURL }

// Open retrieves an object from the remote.
func (c *Remote) Open(ctx context.Context, key Key) (io.ReadCloser, http.Header, error) {
	url := fmt.Sprintf("%s/object/%s", c.baseURL, key.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to create request")
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to execute request")
	}

	if resp.StatusCode == http.StatusNotFound {
		_, _ = io.Copy(io.Discard, resp.Body) //nolint:errcheck,gosec
		return nil, nil, errors.Join(os.ErrNotExist, resp.Body.Close())
	}

	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body) //nolint:errcheck,gosec
		return nil, nil, errors.Join(errors.Errorf("unexpected status code: %d", resp.StatusCode), resp.Body.Close())
	}

	// Filter out HTTP transport headers
	headers := FilterTransportHeaders(resp.Header)

	return resp.Body, headers, nil
}

// Stat retrieves headers for an object from the remote.
func (c *Remote) Stat(ctx context.Context, key Key) (http.Header, error) {
	url := fmt.Sprintf("%s/object/%s", c.baseURL, key.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create request")
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute request")
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, os.ErrNotExist
	}

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Filter out HTTP transport headers
	headers := FilterTransportHeaders(resp.Header)

	return headers, nil
}

// Create stores a new object in the remote.
func (c *Remote) Create(ctx context.Context, key Key, headers http.Header, ttl time.Duration) (io.WriteCloser, error) {
	pr, pw := io.Pipe()

	url := fmt.Sprintf("%s/object/%s", c.baseURL, key.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, pr)
	if err != nil {
		return nil, errors.Join(errors.Wrap(err, "failed to create request"), pr.Close(), pw.Close())
	}

	maps.Copy(req.Header, headers)

	if ttl > 0 {
		req.Header.Set("Time-To-Live", ttl.String())
	}

	wc := &writeCloser{
		pw:   pw,
		done: make(chan error, 1),
		ctx:  ctx,
	}

	go func() {
		resp, err := c.client.Do(req)
		if err != nil {
			wc.done <- errors.Wrap(err, "failed to execute request")
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body) //nolint:errcheck,gosec
		_ = resp.Body.Close()                 //nolint:gosec

		if resp.StatusCode != http.StatusOK {
			wc.done <- errors.Errorf("unexpected status code: %d", resp.StatusCode)
			return
		}

		wc.done <- nil
	}()

	return wc, nil
}

// Delete removes an object from the remote.
func (c *Remote) Delete(ctx context.Context, key Key) error {
	url := fmt.Sprintf("%s/object/%s", c.baseURL, key.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return errors.Wrap(err, "failed to create request")
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to execute request")
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return os.ErrNotExist
	}

	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

// Close closes the client and releases resources.
func (c *Remote) Close() error {
	c.client.CloseIdleConnections()
	return nil
}

// Stats retrieves cache statistics from the remote server.
func (c *Remote) Stats(ctx context.Context) (Stats, error) {
	url := c.baseURL + "/stats"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Stats{}, errors.Wrap(err, "failed to create request")
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return Stats{}, errors.Wrap(err, "failed to execute request")
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotImplemented {
		return Stats{}, ErrStatsUnavailable
	}

	if resp.StatusCode != http.StatusOK {
		return Stats{}, errors.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var stats Stats
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return Stats{}, errors.Wrap(err, "failed to decode stats response")
	}

	return stats, nil
}

// writeCloser wraps a pipe writer and waits for the HTTP request to complete.
type writeCloser struct {
	pw   *io.PipeWriter
	done chan error
	ctx  context.Context
}

func (wc *writeCloser) Write(p []byte) (int, error) {
	n, err := wc.pw.Write(p)
	return n, errors.WithStack(err)
}

func (wc *writeCloser) Close() error {
	if err := wc.ctx.Err(); err != nil {
		_ = wc.pw.CloseWithError(err)
		<-wc.done // Wait for goroutine to finish and release connection
		return errors.Wrap(err, "create operation cancelled")
	}
	if err := wc.pw.Close(); err != nil {
		<-wc.done // Wait for goroutine to finish and release connection
		return errors.Wrap(err, "failed to close pipe writer")
	}
	err := <-wc.done
	if err != nil {
		return errors.Wrap(err, "request failed")
	}
	return nil
}

// Namespace creates a namespaced view (which is ignored by remote cache).
// Remote cache ignores namespaces since the server-side API v1 handles namespacing.
func (c *Remote) Namespace(_ string) Cache {
	// Remote cache doesn't use namespacing on client side
	return c
}

// ListNamespaces requests namespace list from the remote server.
func (c *Remote) ListNamespaces(ctx context.Context) ([]string, error) {
	url := c.baseURL + "/namespaces"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body) //nolint:errcheck
		return nil, errors.Errorf("unexpected status %d: %s", resp.StatusCode, body)
	}

	var namespaces []string
	if err := json.NewDecoder(resp.Body).Decode(&namespaces); err != nil {
		return nil, errors.WithStack(err)
	}

	return namespaces, nil
}
