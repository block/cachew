package handler

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/alecthomas/errors"

	"github.com/block/cachew/client"
)

type parallelFetch struct {
	chunkSize   int64
	concurrency int
}

func (p *parallelFetch) firstChunkRange() string {
	return fmt.Sprintf("bytes=0-%d", p.chunkSize-1)
}

// fetch turns first, the upstream's answer to firstChunkRange, into a
// response carrying the whole object, fetching the remaining chunks in
// parallel. The returned body owns first's body.
func (p *parallelFetch) fetch(ctx context.Context, c *http.Client, first *http.Response) (*http.Response, error) {
	headers := first.Header.Clone()
	headers.Del("Content-Range")
	headers.Del("Content-Length")
	size := contentRangeSize(first.Header.Get("Content-Range"))
	if size >= 0 {
		headers.Set("Content-Length", strconv.FormatInt(size, 10))
	}
	ranges := &upstreamRanges{client: c, request: first.Request, first: first, firstRange: p.firstChunkRange()}
	body, err := client.ParallelGetReader(ctx, ranges, client.Key{}, p.chunkSize, p.concurrency)
	if err != nil {
		return nil, errors.Wrap(err, "parallel fetch")
	}
	return &http.Response{StatusCode: http.StatusOK, Header: headers, Body: body, ContentLength: size}, nil
}

// contentRangeSize returns the total size from a Content-Range header, or -1
// if it is unknown.
func contentRangeSize(contentRange string) int64 {
	_, total, ok := strings.Cut(contentRange, "/")
	if !ok {
		return -1
	}
	size, err := strconv.ParseInt(total, 10, 64)
	if err != nil {
		return -1
	}
	return size
}

// upstreamRanges adapts an upstream URL to client.RangeReader so ParallelGet
// can drive it. Chunks are requested from the final URL of the first
// response, so registries that redirect to a CDN are resolved only once.
type upstreamRanges struct {
	client     *http.Client
	request    *http.Request
	firstRange string

	mu    sync.Mutex
	first *http.Response
}

// Open serves ParallelGet's discovery request from the response the handler
// already received, rather than requesting the first chunk twice.
func (u *upstreamRanges) Open(ctx context.Context, _ client.Key, opts ...client.RequestOption) (io.ReadCloser, http.Header, error) {
	o := client.NewRequestOptions(opts...)
	if first := u.takeFirst(); first != nil {
		if o.Range == u.firstRange {
			return first.Body, first.Header, nil
		}
		_ = first.Body.Close() //nolint:errcheck // The response is replaced by the request below.
	}
	req := u.request.Clone(ctx)
	req.Header.Del("Range")
	req.Header.Del("If-Match")
	if o.Range != "" {
		req.Header.Set("Range", o.Range)
	}
	if o.IfMatch != "" {
		req.Header.Set("If-Match", o.IfMatch)
	}
	resp, err := u.client.Do(req)
	if err != nil {
		return nil, nil, errors.Wrap(err, "fetch range")
	}
	switch resp.StatusCode {
	case http.StatusOK, http.StatusPartialContent:
		return resp.Body, resp.Header, nil
	case http.StatusPreconditionFailed:
		err = client.ErrPreconditionFailed
	case http.StatusRequestedRangeNotSatisfiable:
		err = client.ErrRangeNotSatisfiable
	default:
		err = &client.HTTPStatusError{StatusCode: resp.StatusCode}
	}
	return nil, nil, errors.Join(errors.WithStack(err), resp.Body.Close())
}

func (u *upstreamRanges) takeFirst() *http.Response {
	u.mu.Lock()
	defer u.mu.Unlock()
	first := u.first
	u.first = nil
	return first
}
