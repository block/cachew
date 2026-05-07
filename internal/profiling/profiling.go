// Package profiling starts the Datadog continuous profiler when enabled.
//
// The profiler reads its configuration from the standard DD_* environment
// variables (DD_AGENT_HOST, DD_SERVICE, DD_ENV, DD_VERSION) which are wired
// in by the deployment manifest.
package profiling

import (
	"context"
	"os"

	"github.com/alecthomas/errors"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	ddprofiler "gopkg.in/DataDog/dd-trace-go.v1/profiler"

	"github.com/block/cachew/internal/logging"
)

// Start starts the Datadog continuous profiler when enabled is true.
// Returns a stop function that the caller should defer.
//
// When enabled is false it is a no-op and returns a stop function that
// does nothing.
func Start(ctx context.Context, enabled bool) (stop func(), err error) {
	if !enabled {
		return func() {}, nil
	}

	logger := logging.FromContext(ctx)
	logger.InfoContext(ctx, "Starting Datadog continuous profiler and tracer",
		"dd_service", os.Getenv("DD_SERVICE"),
		"dd_env", os.Getenv("DD_ENV"),
		"dd_version", os.Getenv("DD_VERSION"),
	)

	// Start the tracer with runtime metrics enabled so Go runtime metrics
	// (runtime.go.*) are emitted to DogStatsD. The profiler alone does
	// not emit these. Without contrib instrumentation imports the tracer
	// produces no spans, so this is essentially free.
	tracer.Start(tracer.WithRuntimeMetrics())

	// BlockProfile is intentionally omitted: per DD's docs it can add
	// noticeable CPU overhead and should be opt-in for targeted
	// investigations rather than the default continuous-profiling set.
	if err := ddprofiler.Start(
		ddprofiler.WithProfileTypes(
			ddprofiler.CPUProfile,
			ddprofiler.HeapProfile,
			ddprofiler.GoroutineProfile,
			ddprofiler.MutexProfile,
		),
	); err != nil {
		tracer.Stop()
		return func() {}, errors.Errorf("start datadog profiler: %w", err)
	}

	return func() {
		ddprofiler.Stop()
		tracer.Stop()
	}, nil
}
