package handler

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/alecthomas/assert/v2"
)

func TestDetachedFillContextGrace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		request, cancelRequest := context.WithCancel(t.Context())
		fill, cancelFill := detachedFillContext(request)
		defer cancelFill()
		cancelRequest()
		time.Sleep(fillGrace - time.Second)
		assert.NoError(t, fill.Err())
		time.Sleep(time.Second)
		synctest.Wait()
		assert.IsError(t, fill.Err(), context.Canceled)
	})
}
