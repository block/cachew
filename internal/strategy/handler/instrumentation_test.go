package handler_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"
	"github.com/alecthomas/errors"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/block/cachew/internal/cache"
	"github.com/block/cachew/internal/httputil"
	"github.com/block/cachew/internal/logging"
	"github.com/block/cachew/internal/metadatadb"
	"github.com/block/cachew/internal/metrics"
	"github.com/block/cachew/internal/s3client"
	"github.com/block/cachew/internal/s3client/s3clienttest"
	"github.com/block/cachew/internal/strategy/handler"
)

func artifactReader(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	previous := otel.GetMeterProvider()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(provider)
	t.Cleanup(func() { otel.SetMeterProvider(previous); assert.NoError(t, provider.Shutdown(context.Background())) })
	return reader
}

func artifactCount(t *testing.T, reader *sdkmetric.ManualReader, name string, labels ...string) int64 {
	t.Helper()
	var data metricdata.ResourceMetrics
	assert.NoError(t, reader.Collect(context.Background(), &data))
	var total int64
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			assert.True(t, ok)
			for _, point := range sum.DataPoints {
				match := true
				for i := 0; i < len(labels); i += 2 {
					value, exists := point.Attributes.Value(attribute.Key(labels[i]))
					if !exists || value.Emit() != labels[i+1] {
						match = false
						break
					}
				}

				if match {
					total += point.Value
				}
			}
		}
	}
	return total
}

func TestArtifactCacheSourcesAndBackfill(t *testing.T) {
	reader := artifactReader(t)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "artifact") }))
	defer origin.Close()
	disk, err := cache.NewDisk(artifactContext(t), cache.DiskConfig{Root: t.TempDir()})
	assert.NoError(t, err)
	memory := mustNewMemoryCache()
	metadata := metadatadb.New(artifactContext(t), metadatadb.NewMemoryBackend())
	c := cache.MaybeNewTiered(artifactContext(t), []cache.Cache{disk, memory}, metadata)
	t.Cleanup(func() { assert.NoError(t, c.Close()) })
	h := handler.New(originClient(origin.Client().Transport), c, newTestSpools(t)).Metrics("host", "registry.example").Transform(func(r *http.Request) (*http.Request, error) {
		return http.NewRequestWithContext(r.Context(), http.MethodGet, origin.URL+r.URL.Path, nil)
	})
	serve := func(headers http.Header) (*httptest.ResponseRecorder, map[string]string) {
		r := artifactHTTPRequest(t, "/artifact")
		r.Header = headers
		return serveLabeled(h, r)
	}
	first, labels := serve(http.Header{})
	assert.Equal(t, http.StatusOK, first.Code)
	assert.Equal(t, "artifact", first.Body.String())
	assert.Equal(t, map[string]string{handler.CacheResultAttribute: handler.CacheMiss}, labels)
	second, labels := serve(http.Header{})
	assert.Equal(t, "artifact", second.Body.String())
	assert.Equal(t, "disk", labels[handler.CacheSourceAttribute])
	assert.NoError(t, disk.Delete(artifactContext(t), cache.NewKey("/artifact")))
	third, labels := serve(http.Header{})
	assert.Equal(t, "artifact", third.Body.String())
	assert.Equal(t, map[string]string{handler.CacheResultAttribute: handler.CacheHit, handler.CacheSourceAttribute: "memory"}, labels)
	fourth, labels := serve(http.Header{})
	assert.Equal(t, "artifact", fourth.Body.String())
	assert.Equal(t, "disk", labels[handler.CacheSourceAttribute])
	conditional, labels := serve(http.Header{"If-None-Match": []string{fourth.Header().Get("ETag")}})
	assert.Equal(t, http.StatusNotModified, conditional.Code)
	assert.Equal(t, map[string]string{handler.CacheResultAttribute: handler.CacheHit, handler.CacheSourceAttribute: "disk"}, labels)
	assert.Equal(t, int64(1), artifactCount(t, reader, "cachew.origin.requests", "status_code", "200"))
	assert.Equal(t, int64(1), artifactCount(t, reader, "cachew.cache.fills", "purpose", "origin", "result", "success"))
	assert.Equal(t, int64(1), artifactCount(t, reader, "cachew.cache.fills", "purpose", "backfill", "result", "success"))
	assert.Equal(t, int64(3), artifactCount(t, reader, "cachew.cache.tier_writes", "result", "success"))
}

func TestArtifactRedirectsAndThrottling(t *testing.T) {
	reader := artifactReader(t)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/blocked", http.StatusFound)
			return
		}
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, "rate limited")
	}))
	defer origin.Close()
	c := cache.MaybeNewTiered(artifactContext(t), []cache.Cache{mustNewMemoryCache()}, nil)
	defer c.Close()
	h := handler.New(originClient(origin.Client().Transport), c, newTestSpools(t)).Metrics("host", "registry.example").Transform(func(r *http.Request) (*http.Request, error) {
		return http.NewRequestWithContext(r.Context(), http.MethodGet, origin.URL+"/redirect", nil)
	})
	for range 2 {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, artifactHTTPRequest(t, "/artifact"))
		assert.Equal(t, http.StatusTooManyRequests, w.Code)
		assert.Equal(t, "rate limited", w.Body.String())
	}
	assert.Equal(t, int64(2), artifactCount(t, reader, "cachew.origin.requests", "status_code", "302"))
	assert.Equal(t, int64(2), artifactCount(t, reader, "cachew.origin.requests", "status_code", "429"))
	assert.Equal(t, int64(0), artifactCount(t, reader, "cachew.cache.fills"))
}

type artifactTransportFunc func(*http.Request) (*http.Response, error)

func (f artifactTransportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestArtifactTransportFailures(t *testing.T) {
	for _, tt := range []struct {
		name  string
		err   error
		label string
	}{
		{"connection", errors.New("connection unavailable"), "transport"},
		{"timeout", context.DeadlineExceeded, "timeout"},
		{"canceled", context.Canceled, "canceled"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reader := artifactReader(t)
			client := originClient(artifactTransportFunc(func(*http.Request) (*http.Response, error) { return nil, tt.err }))
			c := mustNewMemoryCache()
			defer c.Close()
			h := handler.New(client, c, newTestSpools(t)).Metrics("host", "registry.example")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, artifactHTTPRequest(t, "http://registry.example/a"))
			assert.Equal(t, http.StatusBadGateway, w.Code)
			assert.Equal(t, int64(1), artifactCount(t, reader, "cachew.origin.requests", "status_code", "none", "error_type", tt.label))
			assert.Equal(t, int64(0), artifactCount(t, reader, "cachew.cache.fills"))
		})
	}
}

type artifactFailCache struct {
	cache.Cache
	phase string
}

func (c artifactFailCache) Create(ctx context.Context, key cache.Key, headers http.Header, ttl time.Duration, opts ...cache.Option) (cache.Writer, error) {
	if c.phase == "create" {
		return nil, errors.New("create failed")
	}
	w, err := c.Cache.Create(ctx, key, headers, ttl, opts...)
	return &artifactFailWriter{Writer: w, phase: c.phase}, err
}

type artifactFailWriter struct {
	cache.Writer
	phase string
}

func (w *artifactFailWriter) Write(p []byte) (int, error) {
	if w.phase == "write" {
		return 0, errors.New("write failed")
	}
	return w.Writer.Write(p)
}
func (w *artifactFailWriter) Close() error {
	if w.phase == "commit" {
		_ = w.Writer.Abort(errors.New("commit failed"))
		return errors.New("commit failed")
	}
	return w.Writer.Close()
}

func TestArtifactFillFailuresKeepClientResponse(t *testing.T) {
	for _, phase := range []string{"create", "write", "commit"} {
		t.Run(phase, func(t *testing.T) {
			reader := artifactReader(t)
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, strings.Repeat("artifact", 10000))
			}))
			defer origin.Close()
			c := cache.MaybeNewTiered(artifactContext(t), []cache.Cache{artifactFailCache{Cache: mustNewMemoryCache(), phase: phase}}, nil)
			defer c.Close()
			h := handler.New(originClient(origin.Client().Transport), c, newTestSpools(t)).Transform(func(r *http.Request) (*http.Request, error) {
				return http.NewRequestWithContext(r.Context(), http.MethodGet, origin.URL, nil)
			})
			w := httptest.NewRecorder()
			h.ServeHTTP(w, artifactHTTPRequest(t, "/artifact"))
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, strings.Repeat("artifact", 10000), w.Body.String())
			_, _, err := c.Open(artifactContext(t), cache.NewKey("/artifact"))
			assert.IsError(t, err, os.ErrNotExist)
			result := phase + "_error"
			assert.Equal(t, int64(1), artifactCount(t, reader, "cachew.cache.fills", "result", result))
			assert.Equal(t, int64(1), artifactCount(t, reader, "cachew.cache.tier_writes", "result", result))
			assert.Equal(t, int64(0), artifactCount(t, reader, "cachew.cache.fills", "result", "success"))
		})
	}
}

func TestArtifactInterruptedBodyIsNotSuccessfulFill(t *testing.T) {
	reader := artifactReader(t)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "10000")
		_, _ = io.WriteString(w, "partial")
	}))
	defer origin.Close()
	c := cache.MaybeNewTiered(artifactContext(t), []cache.Cache{mustNewMemoryCache()}, nil)
	defer c.Close()
	h := handler.New(originClient(origin.Client().Transport), c, newTestSpools(t)).Transform(func(r *http.Request) (*http.Request, error) {
		return http.NewRequestWithContext(r.Context(), http.MethodGet, origin.URL, nil)
	})
	assert.Panics(t, func() { h.ServeHTTP(httptest.NewRecorder(), artifactHTTPRequest(t, "/artifact")) })
	assert.Equal(t, int64(1), artifactCount(t, reader, "cachew.origin.requests", "status_code", "200"))
	assert.Equal(t, int64(1), artifactCount(t, reader, "cachew.cache.fills", "result", "transfer_error"))
	assert.Equal(t, int64(0), artifactCount(t, reader, "cachew.cache.tier_writes", "result", "success"))
}

func TestArtifactInternalForwardAndCardinality(t *testing.T) {
	reader := artifactReader(t)
	client := originClient(artifactTransportFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("artifact"))}, nil
	}))
	c := cache.NoOpCache()
	h := handler.New(client, c, newTestSpools(t)).Metrics("proxy", "other").InternalForward()
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Go(func() {
			w := httptest.NewRecorder()
			h.ServeHTTP(w, artifactHTTPRequest(t, "http://"+strings.Repeat("a", i+1)+".example/secret?token=value"))
			assert.Equal(t, http.StatusOK, w.Code)
		})
	}
	wg.Wait()
	assert.Equal(t, int64(0), artifactCount(t, reader, "cachew.origin.requests"))
	assert.Equal(t, int64(0), artifactCount(t, reader, "cachew.cache.fills"))
	var data metricdata.ResourceMetrics
	assert.NoError(t, reader.Collect(artifactContext(t), &data))
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
				assert.Equal(t, 1, len(sum.DataPoints))
				for _, a := range sum.DataPoints[0].Attributes.ToSlice() {
					assert.False(t, strings.Contains(a.Value.Emit(), ".example"))
					assert.False(t, strings.Contains(a.Value.Emit(), "secret"))
				}
			}
		}
	}
}

func TestArtifactBackfillFailureRemainsVisible(t *testing.T) {
	reader := artifactReader(t)
	local := artifactFailCache{Cache: mustNewMemoryCache(), phase: "write"}
	remote := mustNewMemoryCache()
	key := cache.NewKey("/artifact")
	assert.NoError(t, cache.WriteFunc(artifactContext(t), remote, key, nil, time.Hour, func(w io.Writer) error { _, err := io.WriteString(w, "artifact"); return err }))
	c := cache.MaybeNewTiered(artifactContext(t), []cache.Cache{local, remote}, metadatadb.New(artifactContext(t), metadatadb.NewMemoryBackend()))
	defer c.Close()
	h := handler.New(http.DefaultClient, c, newTestSpools(t))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, artifactHTTPRequest(t, "/artifact"))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "artifact", w.Body.String())
	assert.Equal(t, int64(1), artifactCount(t, reader, "cachew.cache.fills", "purpose", "backfill", "result", "write_error"))
	assert.Equal(t, int64(1), artifactCount(t, reader, "cachew.cache.tier_writes", "purpose", "backfill", "result", "write_error"))
	assert.Equal(t, int64(0), artifactCount(t, reader, "cachew.origin.requests"))
}

func TestArtifactStaleTierIsNotCountedAsHit(t *testing.T) {
	disk, err := cache.NewDisk(artifactContext(t), cache.DiskConfig{Root: t.TempDir()})
	assert.NoError(t, err)
	memory := mustNewMemoryCache()
	key := cache.NewKey("/artifact")
	for _, seed := range []struct {
		c    cache.Cache
		etag string
		body string
	}{{disk, "old", "old"}, {memory, "new", "new"}} {
		assert.NoError(t, cache.WriteFunc(artifactContext(t), seed.c, key, nil, time.Hour, func(w io.Writer) error { _, err := io.WriteString(w, seed.body); return err }, cache.WithETag(seed.etag)))
	}
	c := cache.MaybeNewTiered(artifactContext(t), []cache.Cache{disk, memory}, metadatadb.New(artifactContext(t), metadatadb.NewMemoryBackend()))
	defer c.Close()
	h := handler.New(http.DefaultClient, c, newTestSpools(t))
	r := artifactHTTPRequest(t, "/artifact")
	r.Header.Set("If-Match", `"new"`)
	w, labels := serveLabeled(h, r)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "new", w.Body.String())
	assert.Equal(t, "memory", labels[handler.CacheSourceAttribute])
}

func TestArtifactS3(t *testing.T) {
	reader := artifactReader(t)
	bucket := s3clienttest.Start(t)
	provider := s3client.NewClientProvider(artifactContext(t), s3client.Config{Endpoint: s3clienttest.Addr, UseSSL: false})
	shared, err := cache.NewS3(artifactContext(t), cache.S3Config{Bucket: bucket, MaxTTL: time.Hour, UploadPartSizeMB: 16}, provider)
	assert.NoError(t, err)
	disk, err := cache.NewDisk(artifactContext(t), cache.DiskConfig{Root: t.TempDir()})
	assert.NoError(t, err)
	key := cache.NewKey("/artifact")
	assert.NoError(t, cache.WriteFunc(artifactContext(t), shared, key, nil, time.Hour, func(w io.Writer) error { _, err := io.WriteString(w, "artifact"); return err }))
	c := cache.MaybeNewTiered(artifactContext(t), []cache.Cache{disk, shared}, metadatadb.New(artifactContext(t), metadatadb.NewMemoryBackend()))
	defer c.Close()
	h := handler.New(http.DefaultClient, c, newTestSpools(t)).Metrics("host", "registry.example")
	w, labels := serveLabeled(h, artifactHTTPRequest(t, "/artifact"))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "artifact", w.Body.String())
	assert.Equal(t, "s3", labels[handler.CacheSourceAttribute])
	assert.Equal(t, int64(1), artifactCount(t, reader, "cachew.cache.fills", "purpose", "backfill", "result", "success"))
	assert.Equal(t, int64(0), artifactCount(t, reader, "cachew.origin.requests"))
}

func originClient(rt http.RoundTripper) *http.Client {
	return &http.Client{Transport: metrics.NewOriginTransport(rt)}
}

func serveLabeled(h http.Handler, r *http.Request) (*httptest.ResponseRecorder, map[string]string) {
	labeler := &otelhttp.Labeler{}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r.WithContext(otelhttp.ContextWithLabeler(r.Context(), labeler)))
	labels := map[string]string{}
	for _, kv := range labeler.Get() {
		labels[string(kv.Key)] = kv.Value.Emit()
	}
	return w, labels
}

func artifactContext(t *testing.T) context.Context {
	t.Helper()
	_, ctx := logging.Configure(t.Context(), logging.Config{Level: slog.LevelError})
	return ctx
}

func artifactHTTPRequest(t *testing.T, target string) *http.Request {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, target, nil).WithContext(artifactContext(t))
	r.RequestURI = ""
	return r
}

func TestArtifactMetricsPrometheusExport(t *testing.T) {
	previous := otel.GetMeterProvider()
	exporter, err := metrics.New(artifactContext(t), metrics.Config{ServiceName: "cachew", EnablePrometheus: true})
	assert.NoError(t, err)
	defer func() { otel.SetMeterProvider(previous); assert.NoError(t, exporter.Close()) }()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "artifact") }))
	defer origin.Close()
	c := cache.MaybeNewTiered(artifactContext(t), []cache.Cache{mustNewMemoryCache()}, nil)
	defer c.Close()
	h := handler.New(originClient(origin.Client().Transport), c, newTestSpools(t)).Metrics("host", "registry.example").Transform(func(r *http.Request) (*http.Request, error) {
		return http.NewRequestWithContext(r.Context(), http.MethodGet, origin.URL, nil)
	})
	h.ServeHTTP(httptest.NewRecorder(), artifactHTTPRequest(t, "/artifact"))
	w := httptest.NewRecorder()
	exporter.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	text := w.Body.String()
	for _, name := range []string{"cachew_origin_requests_total{", "cachew_origin_request_duration_seconds_count{", "cachew_cache_fills_total{", "cachew_cache_tier_writes_total{"} {
		assert.True(t, strings.Contains(text, name), "missing Prometheus instrument %s", name)
	}
	assert.True(t, strings.Contains(text, `registry="registry.example"`))
}

func TestArtifactCooldownIsNotOriginTraffic(t *testing.T) {
	reader := artifactReader(t)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer origin.Close()
	client := &http.Client{Transport: httputil.NewBackoffTransport(metrics.NewOriginTransport(origin.Client().Transport), time.Minute, time.Hour, slog.New(slog.DiscardHandler))}
	c := mustNewMemoryCache()
	defer c.Close()
	h := handler.New(client, c, newTestSpools(t)).Metrics("host", "registry.example").Transform(func(r *http.Request) (*http.Request, error) {
		return http.NewRequestWithContext(r.Context(), http.MethodGet, origin.URL, nil)
	})
	for range 3 {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, artifactHTTPRequest(t, "/artifact"))
		assert.Equal(t, http.StatusTooManyRequests, w.Code)
	}
	assert.Equal(t, int64(1), artifactCount(t, reader, "cachew.origin.requests", "status_code", "429"))
}

func TestArtifactHeadHitSource(t *testing.T) {
	disk, err := cache.NewDisk(artifactContext(t), cache.DiskConfig{Root: t.TempDir()})
	assert.NoError(t, err)
	memory := mustNewMemoryCache()
	key := cache.NewKey("/artifact")
	assert.NoError(t, cache.WriteFunc(artifactContext(t), memory, key, nil, time.Hour, func(w io.Writer) error { _, err := io.WriteString(w, "artifact"); return err }))
	for _, tt := range []struct {
		name   string
		c      cache.Cache
		source string
	}{
		{"tiered", cache.MaybeNewTiered(artifactContext(t), []cache.Cache{disk, memory}, metadatadb.New(artifactContext(t), metadatadb.NewMemoryBackend())), "memory"},
		{"single", cache.MaybeNewTiered(artifactContext(t), []cache.Cache{memory}, nil), "memory"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := handler.New(http.DefaultClient, tt.c, newTestSpools(t))
			r := artifactHTTPRequest(t, "/artifact")
			r.Method = http.MethodHead
			w, labels := serveLabeled(h, r)
			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, map[string]string{handler.CacheResultAttribute: handler.CacheHit, handler.CacheSourceAttribute: tt.source}, labels)
		})
	}
}
