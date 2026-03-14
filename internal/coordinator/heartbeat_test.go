package coordinator

import (
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestHeartbeatMonitor_UpdateAndGetRunningTasks(t *testing.T) {
	h := NewHeartbeatMonitor(time.Second, 15*time.Second, zap.NewNop())
	nowMs := time.Now().UnixMilli()

	h.UpdateHeartbeat("w1", nowMs, []string{"task-a", "task-b"})

	tasks := h.GetRunningTasks("w1")
	if len(tasks) != 2 {
		t.Errorf("expected 2 running tasks, got %d: %v", len(tasks), tasks)
	}
}

func TestHeartbeatMonitor_GetRunningTasks_UnknownWorker(t *testing.T) {
	h := NewHeartbeatMonitor(time.Second, 15*time.Second, zap.NewNop())
	tasks := h.GetRunningTasks("unknown")
	if len(tasks) != 0 {
		t.Errorf("expected empty slice for unknown worker, got %v", tasks)
	}
}

func TestHeartbeatMonitor_WorkerCount(t *testing.T) {
	h := NewHeartbeatMonitor(time.Second, 15*time.Second, zap.NewNop())
	h.UpdateHeartbeat("w1", time.Now().UnixMilli(), nil)
	h.UpdateHeartbeat("w2", time.Now().UnixMilli(), nil)

	if h.GetWorkerCount() != 2 {
		t.Errorf("expected 2 workers, got %d", h.GetWorkerCount())
	}
}

func TestHeartbeatMonitor_DeadDetection(t *testing.T) {
	h := NewHeartbeatMonitor(time.Second, 5*time.Second, zap.NewNop())
	// Heartbeat 10 seconds ago — dead
	staleMs := time.Now().Add(-10 * time.Second).UnixMilli()
	h.UpdateHeartbeat("dead-worker", staleMs, nil)

	dead := h.GetDeadWorkers(time.Now().UnixMilli())
	if len(dead) != 1 || dead[0] != "dead-worker" {
		t.Errorf("expected [dead-worker], got %v", dead)
	}
}

func TestHeartbeatMonitor_LiveWorkerNotDead(t *testing.T) {
	h := NewHeartbeatMonitor(time.Second, 15*time.Second, zap.NewNop())
	h.UpdateHeartbeat("live-worker", time.Now().UnixMilli(), nil)

	dead := h.GetDeadWorkers(time.Now().UnixMilli())
	if len(dead) != 0 {
		t.Errorf("expected no dead workers, got %v", dead)
	}
}

func TestHeartbeatMonitor_RemoveWorker(t *testing.T) {
	h := NewHeartbeatMonitor(time.Second, 15*time.Second, zap.NewNop())
	h.UpdateHeartbeat("w1", time.Now().UnixMilli(), nil)
	h.UpdateHeartbeat("w2", time.Now().UnixMilli(), nil)
	h.RemoveWorker("w1")

	if h.GetWorkerCount() != 1 {
		t.Errorf("expected 1 worker after remove, got %d", h.GetWorkerCount())
	}
	tasks := h.GetRunningTasks("w1")
	if len(tasks) != 0 {
		t.Errorf("expected no tasks for removed worker, got %v", tasks)
	}
}

func TestHeartbeatMonitor_Snapshot_HealthyWorker(t *testing.T) {
	h := NewHeartbeatMonitor(time.Second, 15*time.Second, zap.NewNop())
	nowMs := time.Now().UnixMilli()
	h.UpdateHeartbeat("w1", nowMs, []string{"t1"})

	snap := h.Snapshot(nowMs)
	if len(snap) != 1 {
		t.Fatalf("expected 1 snapshot entry, got %d", len(snap))
	}
	if !snap[0].Healthy {
		t.Error("expected worker to be healthy")
	}
	if snap[0].WorkerID != "w1" {
		t.Errorf("expected w1, got %s", snap[0].WorkerID)
	}
	if len(snap[0].RunningTaskIDs) != 1 || snap[0].RunningTaskIDs[0] != "t1" {
		t.Errorf("expected [t1] running, got %v", snap[0].RunningTaskIDs)
	}
}

func TestHeartbeatMonitor_Snapshot_UnhealthyWorker(t *testing.T) {
	h := NewHeartbeatMonitor(time.Second, 5*time.Second, zap.NewNop())
	staleMs := time.Now().Add(-20 * time.Second).UnixMilli()
	h.UpdateHeartbeat("stale", staleMs, nil)

	snap := h.Snapshot(time.Now().UnixMilli())
	if len(snap) != 1 {
		t.Fatalf("expected 1 entry")
	}
	if snap[0].Healthy {
		t.Error("expected stale worker to be unhealthy")
	}
}

func TestHeartbeatMonitor_RunningTasksCopied(t *testing.T) {
	h := NewHeartbeatMonitor(time.Second, 15*time.Second, zap.NewNop())
	h.UpdateHeartbeat("w1", time.Now().UnixMilli(), []string{"task-a"})

	tasks := h.GetRunningTasks("w1")
	// Mutating the returned slice must not affect internal state.
	tasks[0] = "mutated"

	tasks2 := h.GetRunningTasks("w1")
	if tasks2[0] != "task-a" {
		t.Error("GetRunningTasks should return a copy, not a reference")
	}
}
