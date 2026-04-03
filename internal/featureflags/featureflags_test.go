package featureflags_test

import (
	"testing"

	"github.com/alecthomas/assert/v2"

	"github.com/block/cachew/internal/featureflags"
)

func TestBoolFlagDefault(t *testing.T) {
	f := featureflags.New("testbooldefault", true)
	assert.Equal(t, true, f.Get())
}

func TestBoolFlagFromEnv(t *testing.T) {
	t.Setenv("CACHEW_FF_TESTBOOLENV", "false")
	f := featureflags.New("testboolenv", true)
	assert.Equal(t, false, f.Get())
}

func TestIntFlagDefault(t *testing.T) {
	f := featureflags.New("testintdefault", 42)
	assert.Equal(t, 42, f.Get())
}

func TestIntFlagFromEnv(t *testing.T) {
	t.Setenv("CACHEW_FF_TESTINTENV", "99")
	f := featureflags.New("testintenv", 0)
	assert.Equal(t, 99, f.Get())
}

func TestInvalidEnvPanics(t *testing.T) {
	t.Setenv("CACHEW_FF_TESTINVALID", "notanint")
	assert.Panics(t, func() {
		featureflags.New("testinvalid", 0)
	})
}
