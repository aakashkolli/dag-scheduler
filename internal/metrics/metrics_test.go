package metrics_test

import (
	"testing"

	"github.com/aakashkolli/dag-scheduler/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestQueueDepthGauge(t *testing.T) {
	metrics.QueueDepth.Set(5)
	if got := testutil.ToFloat64(metrics.QueueDepth); got != 5 {
		t.Errorf("expected QueueDepth=5, got %v", got)
	}
	metrics.QueueDepth.Set(0)
}

func TestActiveWorkersGauge(t *testing.T) {
	metrics.ActiveWorkers.Set(3)
	if got := testutil.ToFloat64(metrics.ActiveWorkers); got != 3 {
		t.Errorf("expected ActiveWorkers=3, got %v", got)
	}
	metrics.ActiveWorkers.Set(0)
}

func TestTasksCompletedCounter(t *testing.T) {
	before := testutil.ToFloat64(metrics.TasksCompletedTotal)
	metrics.TasksCompletedTotal.Inc()
	after := testutil.ToFloat64(metrics.TasksCompletedTotal)
	if after != before+1 {
		t.Errorf("counter did not increment: before=%v after=%v", before, after)
	}
}

func TestAllMetricsInitialized(t *testing.T) {
	// All metrics must be non-nil (they're initialized via promauto at package init)
	if metrics.QueueDepth == nil {
		t.Error("QueueDepth is nil")
	}
	if metrics.ActiveWorkers == nil {
		t.Error("ActiveWorkers is nil")
	}
	if metrics.TasksCompletedTotal == nil {
		t.Error("TasksCompletedTotal is nil")
	}
	if metrics.TasksFailedTotal == nil {
		t.Error("TasksFailedTotal is nil")
	}
	if metrics.TasksRequeuedTotal == nil {
		t.Error("TasksRequeuedTotal is nil")
	}
	if metrics.WorkerDisconnectsTotal == nil {
		t.Error("WorkerDisconnectsTotal is nil")
	}
	if metrics.TaskDurationMs == nil {
		t.Error("TaskDurationMs is nil")
	}
	if metrics.QueueWaitMs == nil {
		t.Error("QueueWaitMs is nil")
	}
	if metrics.BoltDBTxDurationMs == nil {
		t.Error("BoltDBTxDurationMs is nil")
	}
}
