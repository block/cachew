//go:build linux || darwin

package client

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/alecthomas/assert/v2"
)

func TestPunchHole(t *testing.T) {
	f := newPatternFile(t, 8<<20)
	before := allocatedBytes(t, f)
	assert.True(t, before >= 7<<20, "expected ~8MiB allocated, got %d", before)

	if err := punchHole(f, 0, 4<<20); err != nil {
		t.Skipf("hole punching not supported on this filesystem: %v", err)
	}
	assert.NoError(t, f.Sync())
	after := allocatedBytes(t, f)
	assert.True(t, after <= before-3<<20, "expected at least 3MiB deallocated, before=%d after=%d", before, after)

	fi, err := f.Stat()
	assert.NoError(t, err)
	assert.Equal(t, int64(8<<20), fi.Size())
	buf := make([]byte, 4096)
	_, err = f.ReadAt(buf, 4096)
	assert.NoError(t, err)
	assert.Equal(t, make([]byte, 4096), buf)
}

func TestSpillBufferPunchesConsumedRegions(t *testing.T) {
	requirePunchSupport(t)

	sb, err := newSpillBuffer(t.TempDir())
	assert.NoError(t, err)
	defer sb.Close()

	const size = 16 << 20
	const half = size / 2
	const chunk = 1 << 20
	data := patternBytes(size)
	for off := 0; off < half; off += chunk {
		_, err := sb.WriteAt(data[off:off+chunk], int64(off))
		assert.NoError(t, err)
	}

	gw := &gatedWriter{gateAt: 6 << 20, blocked: make(chan struct{}), release: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- sb.streamTo(context.Background(), gw) }()

	<-gw.blocked
	for off := half; off < size; off += chunk {
		_, err := sb.WriteAt(data[off:off+chunk], int64(off))
		assert.NoError(t, err)
	}
	assert.NoError(t, sb.f.Sync())
	before := allocatedBytes(t, sb.f)
	assert.True(t, before >= size*3/4, "expected ~16MiB allocated, got %d", before)
	assert.NoError(t, sb.closeWrite(nil))
	close(gw.release)

	assert.NoError(t, <-done)
	assert.Equal(t, data, gw.buf.Bytes())
	assert.NoError(t, sb.f.Sync())
	after := allocatedBytes(t, sb.f)
	assert.True(t, after <= size/8, "expected consumed regions deallocated, before=%d after=%d", before, after)
}

// gatedWriter blocks streamTo mid-drain: crossing gateAt closes blocked and
// waits for release, so the test can land more writes and force the first
// punch to happen before the second half is read back. A punch that zeroed
// not-yet-streamed regions would then corrupt the second half of buf.
type gatedWriter struct {
	buf     bytes.Buffer
	gateAt  int
	blocked chan struct{}
	release chan struct{}
	gated   bool
}

func (g *gatedWriter) Write(p []byte) (int, error) {
	if !g.gated && g.buf.Len()+len(p) >= g.gateAt {
		g.gated = true
		close(g.blocked)
		<-g.release
	}
	return g.buf.Write(p)
}

func requirePunchSupport(t *testing.T) {
	t.Helper()
	f := newPatternFile(t, spillPunchAlign)
	if err := punchHole(f, 0, spillPunchAlign); err != nil {
		t.Skipf("hole punching not supported on this filesystem: %v", err)
	}
}

func newPatternFile(t *testing.T, size int64) *os.File {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), "punch"))
	assert.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	_, err = f.Write(patternBytes(int(size)))
	assert.NoError(t, err)
	assert.NoError(t, f.Sync())
	return f
}

func allocatedBytes(t *testing.T, f *os.File) int64 {
	t.Helper()
	fi, err := f.Stat()
	assert.NoError(t, err)
	st, ok := fi.Sys().(*syscall.Stat_t)
	assert.True(t, ok)
	// Blocks counts 512-byte units regardless of the filesystem block size.
	return st.Blocks * 512
}
