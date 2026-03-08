package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	QueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dag_scheduler_queue_depth",
		Help: "Number of tasks currently in the ready queue",
	})

	ActiveWorkers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "dag_scheduler_active_workers",
		Help: "Number of connected workers with recent heartbeats",
	})

	TasksCompletedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dag_scheduler_tasks_completed_total",
		Help: "Total number of tasks that reached COMPLETE state",
	})

	TasksFailedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dag_scheduler_tasks_failed_total",
		Help: "Total number of tasks that reached FAILED state",
	}, []string{"workflow_id", "reason"})

	TasksRequeuedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "dag_scheduler_tasks_requeued_total",
		Help: "Total number of task requeue events",
	}, []string{"reason"})

	WorkerDisconnectsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "dag_scheduler_worker_disconnects_total",
		Help: "Total number of worker disconnect events (heartbeat timeout or stream close)",
	})

	TaskDurationMs = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dag_scheduler_task_duration_ms",
		Help:    "Time from task QUEUED to COMPLETE in milliseconds",
		Buckets: []float64{10, 50, 100, 500, 1000, 5000, 30000},
	}, []string{"workflow_id"})

	QueueWaitMs = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "dag_scheduler_queue_wait_ms",
		Help:    "Time from task becoming QUEUED to worker receipt in milliseconds",
		Buckets: []float64{1, 5, 10, 25, 50, 100, 500},
	})

	BoltDBTxDurationMs = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "dag_scheduler_boltdb_tx_duration_ms",
		Help:    "BoltDB write transaction duration in milliseconds",
		Buckets: []float64{0.5, 1, 2, 5, 10, 20, 50},
	})
)
