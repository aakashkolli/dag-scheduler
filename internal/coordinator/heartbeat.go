package coordinator

import (
	"context"
	"sync"
	"time"

	"github.com/aakashkolli/dag-scheduler/internal/metrics"
	"go.uber.org/zap"
)

// HeartbeatMonitor tracks worker health via explicit timestamps, not context timeouts
type HeartbeatMonitor struct {
	mu            sync.RWMutex
	lastHeartbeat map[string]int64    // worker_id -> last heartbeat unix ms
	runningTasks  map[string][]string // worker_id -> task_ids

	scanInterval  time.Duration
	deadThreshold time.Duration
	logger        *zap.Logger
}

// NewHeartbeatMonitor creates a new heartbeat monitor
func NewHeartbeatMonitor(scanInterval, deadThreshold time.Duration, logger *zap.Logger) *HeartbeatMonitor {
	return &HeartbeatMonitor{
		lastHeartbeat: make(map[string]int64),
		runningTasks:  make(map[string][]string),
		scanInterval:  scanInterval,
		deadThreshold: deadThreshold,
		logger:        logger,
	}
}

// UpdateHeartbeat records a heartbeat from a worker (thread-safe)
func (h *HeartbeatMonitor) UpdateHeartbeat(workerID string, timestampMs int64, runningTasks []string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.lastHeartbeat[workerID] = timestampMs
	h.runningTasks[workerID] = runningTasks
	metrics.ActiveWorkers.Set(float64(len(h.lastHeartbeat)))

	if h.logger != nil {
		h.logger.Debug("worker heartbeat updated",
			zap.String("worker_id", workerID),
			zap.Int("running_tasks", len(runningTasks)),
		)
	}
}

// GetRunningTasks returns the tasks a worker is currently running (thread-safe)
func (h *HeartbeatMonitor) GetRunningTasks(workerID string) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	tasks, exists := h.runningTasks[workerID]
	if !exists {
		return []string{}
	}

	// Return a copy
	result := make([]string, len(tasks))
	copy(result, tasks)
	return result
}

// GetDeadWorkers checks for dead workers based on explicit timestamps
// Returns list of dead worker IDs
func (h *HeartbeatMonitor) GetDeadWorkers(nowMs int64) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var deadWorkers []string

	for workerID, lastHeartbeatMs := range h.lastHeartbeat {
		age := time.Duration(nowMs-lastHeartbeatMs) * time.Millisecond
		if age > h.deadThreshold {
			deadWorkers = append(deadWorkers, workerID)
		}
	}

	return deadWorkers
}

// Run is the main loop that periodically checks for dead workers
// Calls onDeadWorker callback for each dead worker found
func (h *HeartbeatMonitor) Run(ctx context.Context, onDeadWorker func(string) error) error {
	ticker := time.NewTicker(h.scanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			nowMs := time.Now().UnixMilli()
			deadWorkers := h.GetDeadWorkers(nowMs)

			for _, workerID := range deadWorkers {
				if h.logger != nil {
					h.logger.Warn("worker declared dead",
						zap.String("worker_id", workerID),
					)
				}

				if err := onDeadWorker(workerID); err != nil {
					if h.logger != nil {
						h.logger.Error("failed to handle dead worker",
							zap.String("worker_id", workerID),
							zap.Error(err),
						)
					}
					// Continue processing other workers
				}

				// Remove worker from tracking
				h.mu.Lock()
				delete(h.lastHeartbeat, workerID)
				delete(h.runningTasks, workerID)
				h.mu.Unlock()
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// GetWorkerCount returns the number of active workers
func (h *HeartbeatMonitor) GetWorkerCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return len(h.lastHeartbeat)
}

// RemoveWorker removes a worker from tracking (thread-safe)
func (h *HeartbeatMonitor) RemoveWorker(workerID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	delete(h.lastHeartbeat, workerID)
	delete(h.runningTasks, workerID)
	metrics.ActiveWorkers.Set(float64(len(h.lastHeartbeat)))
}

// WorkerHealthSnapshot captures a worker's current heartbeat state.
type WorkerHealthSnapshot struct {
	WorkerID        string   `json:"worker_id"`
	LastHeartbeatMs int64    `json:"last_heartbeat_ms"`
	AgeMs           int64    `json:"age_ms"`
	RunningTaskIDs  []string `json:"running_task_ids"`
	Healthy         bool     `json:"healthy"`
}

// Snapshot returns a point-in-time copy of worker health data.
func (h *HeartbeatMonitor) Snapshot(nowMs int64) []WorkerHealthSnapshot {
	h.mu.RLock()
	defer h.mu.RUnlock()

	result := make([]WorkerHealthSnapshot, 0, len(h.lastHeartbeat))
	for workerID, lastHeartbeatMs := range h.lastHeartbeat {
		running := h.runningTasks[workerID]
		copyRunning := make([]string, len(running))
		copy(copyRunning, running)
		ageMs := nowMs - lastHeartbeatMs
		result = append(result, WorkerHealthSnapshot{
			WorkerID:        workerID,
			LastHeartbeatMs: lastHeartbeatMs,
			AgeMs:           ageMs,
			RunningTaskIDs:  copyRunning,
			Healthy:         time.Duration(ageMs)*time.Millisecond <= h.deadThreshold,
		})
	}

	return result
}
