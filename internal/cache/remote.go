package cache

import (
	"context"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/textproto"
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
	return &Remote{
		baseURL: baseURL + "/api/v1/object",
		client:  &http.Client{},
	}
}

func (c *Remote) String() string { return "remote:" + c.baseURL }

// Open retrieves an object from the remote.
func (c *Remote) Open(ctx context.Context, key Key) (io.ReadCloser, textproto.MIMEHeader, error) {
	url := fmt.Sprintf("%s/%s", c.baseURL, key.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to create request")
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to execute request")
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil, errors.Join(os.ErrNotExist, resp.Body.Close())
	}

	if resp.StatusCode != http.StatusOK {
		return nil, nil, errors.Join(errors.Errorf("unexpected status code: %d", resp.StatusCode), resp.Body.Close())
	}

	// Filter out HTTP transport headers
	headers := FilterTransportHeaders(textproto.MIMEHeader(resp.Header))

	return resp.Body, headers, nil
}

// Create stores a new object in the remote.
func (c *Remote) Create(ctx context.Context, key Key, headers textproto.MIMEHeader, ttl time.Duration) (io.WriteCloser, error) {
	pr, pw := io.Pipe()

	url := fmt.Sprintf("%s/%s", c.baseURL, key.String())
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
		defer resp.Body.Close()

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
func (c *Remote) Close() error {
	c.client.CloseIdleConnections()
	return nil
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
		return errors.Join(errors.Wrap(err, "create operation cancelled"), wc.pw.CloseWithError(err))
	}
	if err := wc.pw.Close(); err != nil {
		return errors.Wrap(err, "failed to close pipe writer")
	}
	err := <-wc.done
	if err != nil {
		return errors.Wrap(err, "request failed")
	}
	return nil
}
