package jobscheduler

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/block/cachew/internal/metrics"
)

type schedulerMetrics struct {
	queueDepth    metric.Int64Gauge
	activeWorkers metric.Int64Gauge
	activeClones  metric.Int64Gauge
	jobsTotal     metric.Int64Counter
	jobDuration   metric.Float64Histogram
}

func newSchedulerMetrics() *schedulerMetrics {
	meter := otel.Meter("cachew.scheduler")
	return &schedulerMetrics{
		queueDepth:    metrics.NewMetric[metric.Int64Gauge](meter, "cachew.scheduler.queue_depth", "{jobs}", "Number of jobs waiting in the scheduler queue"),
		activeWorkers: metrics.NewMetric[metric.Int64Gauge](meter, "cachew.scheduler.active_workers", "{workers}", "Number of workers currently executing jobs"),
		activeClones:  metrics.NewMetric[metric.Int64Gauge](meter, "cachew.scheduler.active_clones", "{jobs}", "Number of clone jobs currently executing"),
		jobsTotal:     metrics.NewMetric[metric.Int64Counter](meter, "cachew.scheduler.jobs_total", "{jobs}", "Total number of completed scheduler jobs"),
		jobDuration:   metrics.NewMetric[metric.Float64Histogram](meter, "cachew.scheduler.job_duration", "s", "Histogram of job durations in seconds"),
	}
}
