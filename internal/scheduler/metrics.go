package scheduler

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/block/cachew/internal/metrics"
)

type schedulerMetrics struct {
	pendingJobs metric.Int64Gauge
	runningJobs metric.Int64Gauge
	jobsTotal   metric.Int64Counter
	jobDuration metric.Float64Histogram
}

func newSchedulerMetrics() *schedulerMetrics {
	meter := otel.Meter("cachew.scheduler")
	return &schedulerMetrics{
		pendingJobs: metrics.NewMetric[metric.Int64Gauge](meter, "cachew.scheduler.pending_jobs", "{jobs}", "Number of jobs waiting in the pending queue"),
		runningJobs: metrics.NewMetric[metric.Int64Gauge](meter, "cachew.scheduler.running_jobs", "{jobs}", "Number of jobs currently executing"),
		jobsTotal:   metrics.NewMetric[metric.Int64Counter](meter, "cachew.scheduler.jobs_total", "{jobs}", "Total number of completed scheduler jobs"),
		jobDuration: metrics.NewMetric[metric.Float64Histogram](meter, "cachew.scheduler.job_duration_seconds", "s", "Duration of scheduler jobs in seconds"),
	}
}
