package git_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/githubapp"
	"github.com/block/cachew/internal/strategy/git"
)

func TestResponseSpoolWriteAndRead(t *testing.T) {
	dir := t.TempDir()
	rs, err := git.NewResponseSpool(filepath.Join(dir, "test.spool"))
	assert.NoError(t, err)

	rs.CaptureHeader(http.StatusOK, http.Header{
		"Content-Type": []string{"application/octet-stream"},
	})
	assert.NoError(t, rs.Write([]byte("hello ")))
	assert.NoError(t, rs.Write([]byte("world")))
	rs.MarkComplete()

	rec := httptest.NewRecorder()
	err = rs.ServeTo(rec)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/octet-stream", rec.Header().Get("Content-Type"))
	assert.Equal(t, "hello world", rec.Body.String())
}

func TestResponseSpoolConcurrentReaders(t *testing.T) {
	dir := t.TempDir()
	rs, err := git.NewResponseSpool(filepath.Join(dir, "test.spool"))
	assert.NoError(t, err)

	rs.CaptureHeader(http.StatusOK, http.Header{})

	const numReaders = 5
	var wg sync.WaitGroup
	recorders := make([]*httptest.ResponseRecorder, numReaders)
	for i := range numReaders {
		recorders[i] = httptest.NewRecorder()
		wg.Add(1)
		go func(rec *httptest.ResponseRecorder) {
			defer wg.Done()
			assert.NoError(t, rs.ServeTo(rec))
		}(recorders[i])
	}

	// Write data in chunks with small delays so readers exercise the blocking path.
	chunks := []string{"chunk1-", "chunk2-", "chunk3"}
	for _, chunk := range chunks {
		time.Sleep(5 * time.Millisecond)
		assert.NoError(t, rs.Write([]byte(chunk)))
	}
	rs.MarkComplete()

	wg.Wait()

	expected := "chunk1-chunk2-chunk3"
	for i, rec := range recorders {
		assert.Equal(t, http.StatusOK, rec.Code, "reader %d status", i)
		assert.Equal(t, expected, rec.Body.String(), "reader %d body", i)
	}
}

func TestResponseSpoolReaderFollowsWriter(t *testing.T) {
	dir := t.TempDir()
	rs, err := git.NewResponseSpool(filepath.Join(dir, "test.spool"))
	assert.NoError(t, err)

	rs.CaptureHeader(http.StatusOK, http.Header{})

	rec := httptest.NewRecorder()
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		assert.NoError(t, rs.ServeTo(rec))
	}()

	// Write progressively and give the reader time to consume.
	for i := range 10 {
		assert.NoError(t, rs.Write([]byte{byte('a' + i)}))
		time.Sleep(2 * time.Millisecond)
	}
	rs.MarkComplete()

	<-readDone
	assert.Equal(t, "abcdefghij", rec.Body.String())
}

func TestResponseSpoolErrorPropagation(t *testing.T) {
	dir := t.TempDir()
	rs, err := git.NewResponseSpool(filepath.Join(dir, "test.spool"))
	assert.NoError(t, err)

	rs.CaptureHeader(http.StatusOK, http.Header{})
	assert.NoError(t, rs.Write([]byte("partial")))

	writeErr := os.ErrClosed
	rs.MarkError(writeErr)

	assert.True(t, rs.Failed())

	rec := httptest.NewRecorder()
	err = rs.ServeTo(rec)
	// Headers were captured before the error, so the reader serves partial data
	// and returns the original error from the read loop.
	assert.IsError(t, err, writeErr)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "partial", rec.Body.String())
}

func TestResponseSpoolErrorBeforeHeader(t *testing.T) {
	dir := t.TempDir()
	rs, err := git.NewResponseSpool(filepath.Join(dir, "test.spool"))
	assert.NoError(t, err)

	rs.MarkError(os.ErrClosed)

	rec := httptest.NewRecorder()
	err = rs.ServeTo(rec)
	// No headers were captured, so ServeTo returns ErrSpoolFailed to allow
	// the caller to fall back to upstream.
	assert.IsError(t, err, git.ErrSpoolFailed)
}

func TestResponseSpoolWaitForReaders(t *testing.T) {
	dir := t.TempDir()
	rs, err := git.NewResponseSpool(filepath.Join(dir, "test.spool"))
	assert.NoError(t, err)

	rs.CaptureHeader(http.StatusOK, http.Header{})

	rec := httptest.NewRecorder()
	readerStarted := make(chan struct{})
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		close(readerStarted)
		_ = rs.ServeTo(rec)
	}()

	<-readerStarted
	time.Sleep(10 * time.Millisecond)

	// Complete the spool so the reader can finish.
	assert.NoError(t, rs.Write([]byte("data")))
	rs.MarkComplete()

	// WaitForReaders should return once the reader goroutine finishes.
	done := make(chan struct{})
	go func() {
		rs.WaitForReaders()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("WaitForReaders timed out")
	}
}

func TestSpoolTeeWriter(t *testing.T) {
	dir := t.TempDir()
	rs, err := git.NewResponseSpool(filepath.Join(dir, "test.spool"))
	assert.NoError(t, err)

	rec := httptest.NewRecorder()
	tw := git.NewSpoolTeeWriter(rec, rs)

	tw.Header().Set("X-Custom", "value")
	tw.WriteHeader(http.StatusCreated)
	_, err = tw.Write([]byte("tee-data"))
	assert.NoError(t, err)
	rs.MarkComplete()

	// Verify original writer got the response.
	assert.Equal(t, http.StatusCreated, rec.Code)
	assert.Equal(t, "value", rec.Header().Get("X-Custom"))
	assert.Equal(t, "tee-data", rec.Body.String())

	// Verify spool captured the response.
	rec2 := httptest.NewRecorder()
	err = rs.ServeTo(rec2)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusCreated, rec2.Code)
	assert.Equal(t, "tee-data", rec2.Body.String())
}

func TestSpoolTeeWriterUpstreamError(t *testing.T) {
	dir := t.TempDir()
	rs, err := git.NewResponseSpool(filepath.Join(dir, "test.spool"))
	assert.NoError(t, err)

	rec := httptest.NewRecorder()
	tw := git.NewSpoolTeeWriter(rec, rs)

	// Upstream returns a 502 — the spool should be marked as failed.
	tw.WriteHeader(http.StatusBadGateway)
	_, err = tw.Write([]byte("bad gateway"))
	assert.NoError(t, err)
	rs.MarkComplete()

	// The original client still gets the error response.
	assert.Equal(t, http.StatusBadGateway, rec.Code)
	assert.Equal(t, "bad gateway", rec.Body.String())

	// The spool should be marked as failed so readers fall back to upstream.
	assert.True(t, rs.Failed())
}

func TestSpoolTeeWriterImplicitHeader(t *testing.T) {
	dir := t.TempDir()
	rs, err := git.NewResponseSpool(filepath.Join(dir, "test.spool"))
	assert.NoError(t, err)

	rec := httptest.NewRecorder()
	tw := git.NewSpoolTeeWriter(rec, rs)

	// Write without explicit WriteHeader; should default to 200.
	_, err = tw.Write([]byte("implicit"))
	assert.NoError(t, err)
	rs.MarkComplete()

	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := httptest.NewRecorder()
	err = rs.ServeTo(rec2)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Equal(t, "implicit", rec2.Body.String())
}

func TestRepoSpoolsGetOrCreate(t *testing.T) {
	dir := t.TempDir()
	rp := git.NewRepoSpools(dir)

	s1, isWriter1, err := rp.GetOrCreate("info-refs")
	assert.NoError(t, err)
	assert.True(t, isWriter1)
	assert.NotZero(t, s1)

	s2, isWriter2, err := rp.GetOrCreate("info-refs")
	assert.NoError(t, err)
	assert.False(t, isWriter2)
	assert.Equal(t, s1, s2)

	s3, isWriter3, err := rp.GetOrCreate("upload-pack")
	assert.NoError(t, err)
	assert.True(t, isWriter3)
	assert.NotEqual(t, s1, s3)
}

func TestRepoSpoolsClose(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "spooldir")
	rp := git.NewRepoSpools(dir)

	s1, _, err := rp.GetOrCreate("info-refs")
	assert.NoError(t, err)
	s1.CaptureHeader(http.StatusOK, http.Header{})
	assert.NoError(t, s1.Write([]byte("data")))
	s1.MarkComplete()

	assert.NoError(t, rp.Close())

	// Spool directory should be removed.
	_, err = os.Stat(dir)
	assert.True(t, os.IsNotExist(err))

	// Further GetOrCreate calls should fail.
	_, _, err = rp.GetOrCreate("upload-pack")
	assert.Error(t, err)
}

func TestRepoSpoolsCloseWaitsForReaders(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "spooldir")
	rp := git.NewRepoSpools(dir)

	s1, _, err := rp.GetOrCreate("test")
	assert.NoError(t, err)
	s1.CaptureHeader(http.StatusOK, http.Header{})

	rec := httptest.NewRecorder()
	readerRunning := make(chan struct{})
	go func() {
		close(readerRunning)
		_ = s1.ServeTo(rec)
	}()

	<-readerRunning
	time.Sleep(10 * time.Millisecond)

	closed := make(chan struct{})
	go func() {
		assert.NoError(t, rp.Close())
		close(closed)
	}()

	// Close should block because a reader is active.
	select {
	case <-closed:
		t.Fatal("Close returned before reader finished")
	case <-time.After(50 * time.Millisecond):
	}

	// Complete the write so the reader can finish.
	assert.NoError(t, s1.Write([]byte("ok")))
	s1.MarkComplete()

	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close timed out after reader finished")
	}
}

func TestSpoolKeyForRequest(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		method   string
		body     string
		expected string
	}{
		{name: "InfoRefs", path: "org/repo.git/info/refs", method: http.MethodGet, expected: ""},
		{name: "UploadPackGET", path: "org/repo.git/git-upload-pack", method: http.MethodGet, expected: "upload-pack"},
		{name: "Unknown", path: "org/repo.git/something-else", method: http.MethodGet, expected: ""},
		{name: "Plain", path: "org/repo", method: http.MethodGet, expected: ""},
		{name: "UploadPackPOSTSameBody", path: "org/repo.git/git-upload-pack", method: http.MethodPost, body: "command=ls-refs\n", expected: ""},
		{name: "UploadPackPOSTDiffBody", path: "org/repo.git/git-upload-pack", method: http.MethodPost, body: "command=fetch\n", expected: ""},
	}

	// Compute expected keys for the two POST cases by running them first.
	var lsRefsKey, fetchKey string
	for i, tt := range tests {
		r := httptest.NewRequest(tt.method, "/"+tt.path, strings.NewReader(tt.body))
		key, err := git.SpoolKeyForRequest(tt.path, r)
		assert.NoError(t, err)
		switch tt.name {
		case "UploadPackPOSTSameBody":
			lsRefsKey = key
			tests[i].expected = key
		case "UploadPackPOSTDiffBody":
			fetchKey = key
			tests[i].expected = key
		}
	}
	// The two POST keys must differ (different bodies).
	assert.NotEqual(t, lsRefsKey, fetchKey)
	// Both must start with "upload-pack-" prefix.
	assert.Contains(t, lsRefsKey, "upload-pack-")
	assert.Contains(t, fetchKey, "upload-pack-")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(tt.method, "/"+tt.path, strings.NewReader(tt.body))
			key, err := git.SpoolKeyForRequest(tt.path, r)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, key)
		})
	}
}

func TestResponseSpoolLargeData(t *testing.T) {
	dir := t.TempDir()
	rs, err := git.NewResponseSpool(filepath.Join(dir, "large.spool"))
	assert.NoError(t, err)

	rs.CaptureHeader(http.StatusOK, http.Header{})

	// Write 1MB in 4KB chunks.
	chunk := make([]byte, 4096)
	for i := range chunk {
		chunk[i] = byte(i % 256)
	}
	totalChunks := 256
	totalSize := len(chunk) * totalChunks

	rec := httptest.NewRecorder()
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		assert.NoError(t, rs.ServeTo(rec))
	}()

	for range totalChunks {
		assert.NoError(t, rs.Write(chunk))
	}
	rs.MarkComplete()

	<-readDone
	assert.Equal(t, totalSize, rec.Body.Len())
}
