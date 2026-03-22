package coordinator

import (
	"context"
	"time"

	"github.com/aakashkolli/dag-scheduler/internal/proto/scheduler"
	"go.uber.org/zap"
)

const (
	watchdogScanInterval  = 30 * time.Second
	watchdogHungThreshold = 10 * time.Minute
)

// runWatchdog alerts on workflows that have been RUNNING longer than watchdogHungThreshold
// without reaching a terminal state. This satisfies PRD invariant #1: no workflow hangs
// indefinitely without detection.
func (c *Coordinator) runWatchdog(ctx context.Context) {
	ticker := time.NewTicker(watchdogScanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.checkHungWorkflows()
		}
	}
}

func (c *Coordinator) checkHungWorkflows() {
	workflows, err := c.state.GetAllWorkflowStates()
	if err != nil {
		return
	}

	nowMs := time.Now().UnixMilli()
	thresholdMs := watchdogHungThreshold.Milliseconds()

	for _, wf := range workflows {
		if wf.State != scheduler.WorkflowState_WORKFLOW_RUNNING {
			continue
		}
		ageMs := nowMs - wf.SubmittedAtMs
		if ageMs > thresholdMs {
			if c.logger != nil {
				c.logger.Warn("workflow appears hung — no terminal state reached",
					zap.String("workflow_id", wf.WorkflowID),
					zap.Int64("age_seconds", ageMs/1000),
					zap.Int64("threshold_seconds", thresholdMs/1000),
				)
			}
		}
	}
}
