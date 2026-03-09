package coordinator

import (
	"time"

	"github.com/aakashkolli/dag-scheduler/internal/proto/scheduler"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// PrepareRecovery marks all RUNNING tasks as QUEUED and re-enqueues them.
// By doing this immediately, we assume that any task running during a
// coordinator crash was lost. This removes the complex reconciliation window.
func (c *Coordinator) PrepareRecovery() error {
	c.metricsMu.Lock()
	c.recoveryPrepareCount++
	c.lastRecoveryStartedAtMs = time.Now().UnixMilli()
	c.metricsMu.Unlock()

	if c.logger != nil {
		c.logger.Info("starting crash recovery: re-enqueuing RUNNING tasks")
	}

	runningTasks, err := c.state.GetTasksInState(scheduler.TaskState_TASK_RUNNING)
	if err != nil {
		return err
	}

	for _, taskID := range runningTasks {
		newExecID := uuid.New().String()
		if err := c.state.RequeueTask(taskID, newExecID); err != nil {
			if c.logger != nil {
				c.logger.Error("failed to requeue task during recovery",
					zap.String("task_id", taskID),
					zap.Error(err),
				)
			}
			continue
		}

		c.mu.Lock()
		c.readyQueue = append(c.readyQueue, taskID)
		c.mu.Unlock()
	}

	if c.logger != nil {
		c.logger.Info("recovered and re-enqueued running tasks",
			zap.Int("task_count", len(runningTasks)),
		)
	}

	return nil
}
