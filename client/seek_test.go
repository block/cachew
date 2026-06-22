package client_test

import (
	"io"
	"strings"
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/client"
)

func TestSeekReadCloser(t *testing.T) {
	const content = "0123456789abcdefghij" // 20 bytes
	size := int64(len(content))

	// openAt returns the tail of content starting at offset, mimicking a ranged
	// open.
	openAt := func(offset int64) (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(content[offset:])), nil
	}

	t.Run("OpensOnceAtSeekedOffset", func(t *testing.T) {
		opens := []int64{}
		open := func(offset int64) (io.ReadCloser, error) {
			opens = append(opens, offset)
			return openAt(offset)
		}
		r := client.NewSeekReadCloser(size, open)
		_, err := r.Seek(0, io.SeekEnd) // free, no open
		assert.NoError(t, err)
		off, err := r.Seek(10, io.SeekStart) // free, final offset wins
		assert.NoError(t, err)
		assert.Equal(t, int64(10), off)

		got, err := io.ReadAll(r)
		assert.NoError(t, err)
		assert.NoError(t, r.Close())
		assert.Equal(t, content[10:], string(got))
		assert.Equal(t, []int64{10}, opens) // exactly one open, at the seeked offset
	})

	t.Run("SequentialReadFromStart", func(t *testing.T) {
		r := client.NewSeekReadCloser(size, openAt)
		got, err := io.ReadAll(r)
		assert.NoError(t, err)
		assert.NoError(t, r.Close())
		assert.Equal(t, content, string(got))
	})

	t.Run("SeekEndResolvesAgainstSize", func(t *testing.T) {
		r := client.NewSeekReadCloser(size, openAt)
		off, err := r.Seek(-5, io.SeekEnd)
		assert.NoError(t, err)
		assert.Equal(t, int64(15), off)
		got, err := io.ReadAll(r)
		assert.NoError(t, err)
		assert.Equal(t, content[15:], string(got))
	})

	t.Run("SeekAfterReadFails", func(t *testing.T) {
		r := client.NewSeekReadCloser(size, openAt)
		buf := make([]byte, 4)
		_, err := io.ReadFull(r, buf)
		assert.NoError(t, err)
		_, err = r.Seek(0, io.SeekStart)
		assert.Error(t, err)
	})

	t.Run("SeekToEndReadsNothing", func(t *testing.T) {
		opened := false
		open := func(offset int64) (io.ReadCloser, error) {
			opened = true
			return openAt(offset)
		}
		r := client.NewSeekReadCloser(size, open)
		off, err := r.Seek(0, io.SeekEnd)
		assert.NoError(t, err)
		assert.Equal(t, size, off)
		got, err := io.ReadAll(r)
		assert.NoError(t, err)
		assert.NoError(t, r.Close())
		assert.Equal(t, "", string(got))
		assert.False(t, opened) // no ranged open issued at EOF
	})

	t.Run("SeekPastEndReturnsRequestedOffsetAndReadsNothing", func(t *testing.T) {
		opened := false
		open := func(offset int64) (io.ReadCloser, error) {
			opened = true
			return openAt(offset)
		}
		r := client.NewSeekReadCloser(size, open)
		// io.Seeker: a past-end seek is allowed and returns the requested
		// offset (not capped); only the subsequent read is affected.
		off, err := r.Seek(size+100, io.SeekStart)
		assert.NoError(t, err)
		assert.Equal(t, size+100, off)
		got, err := io.ReadAll(r)
		assert.NoError(t, err)
		assert.Equal(t, "", string(got))
		assert.False(t, opened) // no ranged open issued past EOF
	})

	t.Run("SeekBeforeStartFails", func(t *testing.T) {
		r := client.NewSeekReadCloser(size, openAt)
		_, err := r.Seek(-1, io.SeekStart)
		assert.Error(t, err)
	})
}
