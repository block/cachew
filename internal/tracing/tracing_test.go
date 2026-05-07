package tracing_test

import (
	"context"
	"testing"

	"github.com/block/cachew/internal/tracing"
)

// New is a no-op when disabled and must return a non-nil stop function.
func TestNewDisabled(t *testing.T) {
	stop, err := tracing.New(context.Background(), tracing.Config{Enabled: false})
	if err != nil {
		t.Fatalf("New(disabled) returned error: %v", err)
	}
	if stop == nil {
		t.Fatal("New(disabled) returned nil stop func")
	}
	stop()
}
