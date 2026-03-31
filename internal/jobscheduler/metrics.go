package jobscheduler

import (
	"github.com/alecthomas/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

type schedulerMetrics struct {
	queueDepth    metric.Int64Gauge
	activeWorkers metric.Int64Gauge
	activeCost    metric.Int64Gauge
	activeClones  metric.Int64Gauge
	jobsTotal     metric.Int64Counter
	jobDuration   metric.Float64Histogram
}

func newSchedulerMetrics() (*schedulerMetrics, error) {
	meter := otel.Meter("cachew.scheduler")
	m := &schedulerMetrics{}
	var err error

	if m.queueDepth, err = meter.Int64Gauge("cachew.scheduler.queue_depth",
		metric.WithDescription("Number of jobs waiting in the scheduler queue"),
		metric.WithUnit("{jobs}")); err != nil {
		return nil, errors.Wrap(err, "create queue_depth gauge")
	}

	if m.activeWorkers, err = meter.Int64Gauge("cachew.scheduler.active_workers",
		metric.WithDescription("Number of workers currently executing jobs"),
		metric.WithUnit("{workers}")); err != nil {
		return nil, errors.Wrap(err, "create active_workers gauge")
	}

	if m.activeCost, err = meter.Int64Gauge("cachew.scheduler.active_cost",
		metric.WithDescription("Total cost of currently executing jobs"),
		metric.WithUnit("{cost}")); err != nil {
		return nil, errors.Wrap(err, "create active_cost gauge")
	}

	if m.activeClones, err = meter.Int64Gauge("cachew.scheduler.active_clones",
		metric.WithDescription("Number of clone jobs currently executing"),
		metric.WithUnit("{jobs}")); err != nil {
		return nil, errors.Wrap(err, "create active_clones gauge")
	}

	if m.jobsTotal, err = meter.Int64Counter("cachew.scheduler.jobs_total",
		metric.WithDescription("Total number of completed scheduler jobs"),
		metric.WithUnit("{jobs}")); err != nil {
		return nil, errors.Wrap(err, "create jobs_total counter")
	}

	if m.jobDuration, err = meter.Float64Histogram("cachew.scheduler.job_duration_seconds",
		metric.WithDescription("Duration of scheduler jobs in seconds"),
		metric.WithUnit("s")); err != nil {
		return nil, errors.Wrap(err, "create job_duration histogram")
	}

	return m, nil
}
