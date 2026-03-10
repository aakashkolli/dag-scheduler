package coordinator

import (
	"context"
	"time"

	"github.com/aakashkolli/dag-scheduler/internal/proto/scheduler"
	"go.uber.org/zap"
)

const (
	watchdogScanInterval  = 30 * time.Second
	watchdogHungThreshold = 5 * time.Minute
)

// runWatchdog alerts on workflows that have been RUNNING longer than watchdogHungThreshold.
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

	for _, wf := range workflows {
		if wf.State != scheduler.WorkflowState_WORKFLOW_RUNNING {
			continue
		}
		if nowMs-wf.SubmittedAtMs > watchdogHungThreshold.Milliseconds() {
			if c.logger != nil {
				c.logger.Warn("workflow appears hung",
					zap.String("workflow_id", wf.WorkflowID),
					zap.Int64("age_seconds", (nowMs-wf.SubmittedAtMs)/1000),
				)
			}
		}
	}
}
