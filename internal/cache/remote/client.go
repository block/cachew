package remote

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/alecthomas/errors"

	"github.com/block/sfptc/internal/cache"
)

// Client implements cache.Cache as a client for the remote cache server.
type Client struct {
	baseURL string
	client  *http.Client
}

var _ cache.Cache = (*Client)(nil)

// NewClient creates a new remote cache client.
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		client:  &http.Client{},
	}
}

// Open retrieves an object from the remote cache.
func (c *Client) Open(ctx context.Context, key cache.Key) (io.ReadCloser, error) {
	url := fmt.Sprintf("%s/%s", c.baseURL, key.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create request")
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute request")
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, errors.Join(os.ErrNotExist, resp.Body.Close())
	}

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Join(errors.Errorf("unexpected status code: %d", resp.StatusCode), resp.Body.Close())
	}

	return resp.Body, nil
}

// Create stores a new object in the remote cache.
func (c *Client) Create(ctx context.Context, key cache.Key, ttl time.Duration) (io.WriteCloser, error) {
	pr, pw := io.Pipe()

	url := fmt.Sprintf("%s/%s", c.baseURL, key.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, pr)
	if err != nil {
		return nil, errors.Join(errors.Wrap(err, "failed to create request"), pr.Close(), pw.Close())
	}

	if ttl > 0 {
		req.Header.Set("Time-To-Live", ttl.String())
	}

	wc := &writeCloser{
		pw:   pw,
		done: make(chan error, 1),
	}

	go func() {
		resp, err := c.client.Do(req)
		if err != nil {
			wc.done <- errors.Wrap(err, "failed to execute request")
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			wc.done <- errors.Errorf("unexpected status code: %d", resp.StatusCode)
			return
		}

		wc.done <- nil
	}()

	return wc, nil
}

// Delete removes an object from the remote cache.
func (c *Client) Delete(ctx context.Context, key cache.Key) error {
	url := fmt.Sprintf("%s/%s", c.baseURL, key.String())
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
func (c *Client) Close() error {
	c.client.CloseIdleConnections()
	return nil
}

// writeCloser wraps a pipe writer and waits for the HTTP request to complete.
type writeCloser struct {
	pw   *io.PipeWriter
	done chan error
}

func (wc *writeCloser) Write(p []byte) (int, error) {
	n, err := wc.pw.Write(p)
	return n, errors.WithStack(err)
}

func (wc *writeCloser) Close() error {
	if err := wc.pw.Close(); err != nil {
		return errors.Wrap(err, "failed to close pipe writer")
	}
	err := <-wc.done
	if err != nil {
		return errors.Wrap(err, "request failed")
	}
	return nil
}
