package coordinator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aakashkolli/dag-scheduler/internal/metrics"
	"github.com/aakashkolli/dag-scheduler/internal/proto/scheduler"
	"github.com/aakashkolli/dag-scheduler/internal/store"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// Coordinator orchestrates task execution across workers
type Coordinator struct {
	state         *store.StateStore
	heartbeat     *HeartbeatMonitor
	logger        *zap.Logger
	mu            sync.RWMutex
	readyQueue    []string          // task_ids ready to execute
	runningTasks  map[string]string // task_id -> worker_id
	assignedTasks map[string]string // task_id -> execution_id

	// Caches populated at SubmitWorkflow time to avoid repeated BoltDB reads.
	taskSpecCache         map[string]*scheduler.TaskSpec     // task_id -> spec
	taskWorkflowMap       map[string]string                  // task_id -> workflow_id
	workflowFailurePolicy map[string]scheduler.FailurePolicy // workflow_id -> policy

	metricsMu              sync.RWMutex
	deadWorkersHandled     int
	recoveryPrepareCount   int
	lastDeadWorkerAtMs     int64
	lastRecoveryStartedAtMs int64

	ctx    context.Context
	cancel context.CancelFunc
}

// MetricsSnapshot captures live coordinator counters for dashboards.
type MetricsSnapshot struct {
	ReadyQueueSize       int   `json:"ready_queue_size"`
	RunningTaskCount     int   `json:"running_task_count"`
	AssignedTaskCount    int   `json:"assigned_task_count"`
	WorkerCount          int   `json:"worker_count"`
	DeadWorkersHandled   int   `json:"dead_workers_handled"`
	RecoveryPrepareCount int   `json:"recovery_prepare_count"`
	LastDeadWorkerAtMs   int64 `json:"last_dead_worker_at_ms"`
}

// Option configures a Coordinator.
type Option func(*Coordinator)

// WithHeartbeatConfig overrides the heartbeat scan interval and dead threshold.
func WithHeartbeatConfig(scan, dead time.Duration) Option {
	return func(c *Coordinator) {
		c.heartbeat = NewHeartbeatMonitor(scan, dead, c.logger)
	}
}

// NewCoordinator creates a new coordinator
func NewCoordinator(state *store.StateStore, logger *zap.Logger, opts ...Option) *Coordinator {
	ctx, cancel := context.WithCancel(context.Background())

	c := &Coordinator{
		state:                 state,
		logger:                logger,
		heartbeat:             NewHeartbeatMonitor(5*time.Second, 15*time.Second, logger),
		readyQueue:            []string{},
		runningTasks:          make(map[string]string),
		assignedTasks:         make(map[string]string),
		taskSpecCache:         make(map[string]*scheduler.TaskSpec),
		taskWorkflowMap:       make(map[string]string),
		workflowFailurePolicy: make(map[string]scheduler.FailurePolicy),
		ctx:                   ctx,
		cancel:                cancel,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// SnapshotMetrics returns coordinator queue and recovery counters.
func (c *Coordinator) SnapshotMetrics() MetricsSnapshot {
	c.mu.RLock()
	readyQueueSize := len(c.readyQueue)
	runningTaskCount := len(c.runningTasks)
	assignedTaskCount := len(c.assignedTasks)
	c.mu.RUnlock()

	metrics := MetricsSnapshot{
		ReadyQueueSize:    readyQueueSize,
		RunningTaskCount:  runningTaskCount,
		AssignedTaskCount: assignedTaskCount,
		WorkerCount:       c.heartbeat.GetWorkerCount(),
	}

	c.metricsMu.RLock()
	metrics.DeadWorkersHandled = c.deadWorkersHandled
	metrics.RecoveryPrepareCount = c.recoveryPrepareCount
	metrics.LastDeadWorkerAtMs = c.lastDeadWorkerAtMs
	c.metricsMu.RUnlock()

	return metrics
}

// Start initializes the coordinator and starts background processes
func (c *Coordinator) Start() error {
	go c.heartbeat.Run(c.ctx, c.HandleDeadWorker)
	go c.runWatchdog(c.ctx)

	if c.logger != nil {
		c.logger.Info("coordinator started")
	}

	return nil
}

// Stop gracefully stops the coordinator
func (c *Coordinator) Stop() error {
	c.cancel()
	if c.logger != nil {
		c.logger.Info("coordinator stopped")
	}
	return nil
}

// LoadStateFromDB hydrates in-memory caches and the ready queue from BoltDB on restart.
// This prevents in-flight workflows from hanging if the coordinator crashes.
func (c *Coordinator) LoadStateFromDB() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	workflows, err := c.state.GetAllWorkflowStates()
	if err != nil {
		return err
	}

	activeCount := 0
	for _, wfState := range workflows {
		if wfState.State != scheduler.WorkflowState_WORKFLOW_RUNNING {
			continue
		}

		wfSpec, err := c.state.GetWorkflow(wfState.WorkflowID)
		if err != nil {
			continue
		}

		c.workflowFailurePolicy[wfSpec.WorkflowId] = wfSpec.FailurePolicy
		for _, task := range wfSpec.Tasks {
			c.taskSpecCache[task.TaskId] = task
			c.taskWorkflowMap[task.TaskId] = wfSpec.WorkflowId
		}
		activeCount++
	}

	queuedTasks, err := c.state.GetTasksInState(scheduler.TaskState_TASK_QUEUED)
	if err != nil {
		return err
	}
	c.readyQueue = append(c.readyQueue, queuedTasks...)

	if c.logger != nil {
		c.logger.Info("state hydrated from DB",
			zap.Int("queued_tasks", len(queuedTasks)),
			zap.Int("active_workflows", activeCount),
		)
	}

	return nil
}

// SubmitWorkflow submits a new workflow for execution
func (c *Coordinator) SubmitWorkflow(workflow *scheduler.WorkflowSpec) error {
	dag, err := ValidateAndSort(workflow)
	if err != nil {
		return fmt.Errorf("invalid DAG: %w", err)
	}

	if err := c.state.StoreWorkflow(workflow); err != nil {
		return err
	}

	if err := c.state.InitWorkflowState(workflow.WorkflowId); err != nil {
		return err
	}

	if err := c.state.InitializeTasksForWorkflow(
		workflow.WorkflowId,
		workflow.Tasks,
		dag.GetInDegree(),
		dag.GetAllDownstream(),
	); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Populate in-memory caches
	for taskID, spec := range dag.GetAllTaskSpecs() {
		c.taskSpecCache[taskID] = spec
		c.taskWorkflowMap[taskID] = workflow.WorkflowId
	}
	c.workflowFailurePolicy[workflow.WorkflowId] = workflow.FailurePolicy

	for _, taskID := range dag.GetReadyTasks() {
		if err := c.state.UpdateTaskState(taskID, scheduler.TaskState_TASK_QUEUED); err != nil {
			return err
		}
		c.readyQueue = append(c.readyQueue, taskID)
	}

	if c.logger != nil {
		c.logger.Info("workflow submitted",
			zap.String("workflow_id", workflow.WorkflowId),
			zap.Int("task_count", len(workflow.Tasks)),
			zap.Int("ready_tasks", len(dag.GetReadyTasks())),
		)
	}

	return nil
}

// GetReadyTask returns the next ready task, or nil if none available
func (c *Coordinator) GetReadyTask(workerID string) *scheduler.TaskAssignment {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.readyQueue) == 0 {
		return nil
	}

	taskID := c.readyQueue[0]
	c.readyQueue = c.readyQueue[1:]

	// Use in-memory cache: no BoltDB reads, no O(n) spec scan
	taskSpec, ok := c.taskSpecCache[taskID]
	if !ok {
		if c.logger != nil {
			c.logger.Error("task spec not found in cache", zap.String("task_id", taskID))
		}
		return nil
	}

	executionID := uuid.New().String()

	if err := c.state.AssignTask(taskID, executionID); err != nil {
		if c.logger != nil {
			c.logger.Error("failed to assign task", zap.Error(err))
		}
		return nil
	}

	c.runningTasks[taskID] = workerID
	c.assignedTasks[taskID] = executionID
	metrics.QueueDepth.Set(float64(len(c.readyQueue)))

	if c.logger != nil {
		c.logger.Debug("task assigned",
			zap.String("task_id", taskID),
			zap.String("worker_id", workerID),
			zap.String("execution_id", executionID),
		)
	}

	return &scheduler.TaskAssignment{
		TaskId:         taskID,
		ExecutionId:    executionID,
		Command:        taskSpec.Command,
		Env:            taskSpec.Env,
		TimeoutSeconds: taskSpec.TimeoutSeconds,
	}
}

// CompleteTask marks a task as complete
func (c *Coordinator) CompleteTask(taskID string, executionID string, output string) error {
	c.mu.Lock()

	assignedExecID, exists := c.assignedTasks[taskID]
	if !exists {
		c.mu.Unlock()
		task, err := c.state.GetTask(taskID)
		if err != nil {
			return err
		}
		if task.State == scheduler.TaskState_TASK_COMPLETE {
			if task.ExecutionID != executionID {
				return store.ErrStaleExecution
			}
			return nil // idempotent
		}
		return store.ErrStaleExecution
	}

	if assignedExecID != executionID {
		c.mu.Unlock()
		if c.logger != nil {
			c.logger.Warn("stale execution result",
				zap.String("task_id", taskID),
				zap.String("expected", assignedExecID),
				zap.String("received", executionID),
			)
		}
		return store.ErrStaleExecution
	}

	c.mu.Unlock()

	// Persist completion atomically BEFORE cleaning up in-memory state.
	// If this fails, the task remains in assignedTasks and a retry will be accepted.
	downstreamReady, err := c.state.CompleteTask(taskID, executionID, output)
	if err != nil {
		return err
	}

	c.mu.Lock()
	delete(c.runningTasks, taskID)
	delete(c.assignedTasks, taskID)
	c.readyQueue = append(c.readyQueue, downstreamReady...)
	metrics.QueueDepth.Set(float64(len(c.readyQueue)))
	c.mu.Unlock()

	metrics.TasksCompletedTotal.Inc()

	if c.logger != nil {
		c.logger.Info("task completed",
			zap.String("task_id", taskID),
			zap.Int("downstream_ready", len(downstreamReady)),
		)
	}

	workflowID := c.taskWorkflowID(taskID)
	if workflowID != "" {
		c.checkWorkflowCompletion(workflowID)
	}

	return nil
}

// FailTask marks a task as failed and applies the workflow's failure policy.
func (c *Coordinator) FailTask(taskID string, executionID string, errMsg string) error {
	c.mu.Lock()

	assignedExecID, exists := c.assignedTasks[taskID]
	if !exists || assignedExecID != executionID {
		c.mu.Unlock()
		return store.ErrStaleExecution
	}

	c.mu.Unlock()

	task, err := c.state.GetTask(taskID)
	if err != nil {
		return err
	}

	// Determine max retries from cached spec
	var maxRetries int32 = 3
	c.mu.RLock()
	if spec, ok := c.taskSpecCache[taskID]; ok && spec.MaxRetries > 0 {
		maxRetries = int32(spec.MaxRetries)
	}
	c.mu.RUnlock()

	task.AttemptCount++

	if task.AttemptCount < maxRetries {
		execID := uuid.New().String()
		if err := c.state.RequeueTask(taskID, execID); err != nil {
			return err
		}

		c.mu.Lock()
		delete(c.runningTasks, taskID)
		delete(c.assignedTasks, taskID)
		c.readyQueue = append(c.readyQueue, taskID)
		metrics.QueueDepth.Set(float64(len(c.readyQueue)))
		c.mu.Unlock()

		metrics.TasksRequeuedTotal.WithLabelValues("retry").Inc()

		if c.logger != nil {
			c.logger.Info("task will be retried",
				zap.String("task_id", taskID),
				zap.Int32("attempt", task.AttemptCount),
				zap.Int32("max_retries", maxRetries),
			)
		}

		return nil
	}

	// Retries exhausted — persist FAILED state atomically
	if err := c.state.FailTask(taskID, errMsg, task.AttemptCount); err != nil {
		return err
	}

	c.mu.Lock()
	delete(c.runningTasks, taskID)
	delete(c.assignedTasks, taskID)
	workflowID := c.taskWorkflowMap[taskID]
	policy := c.workflowFailurePolicy[workflowID]
	c.mu.Unlock()

	metrics.TasksFailedTotal.WithLabelValues(workflowID, "retries_exhausted").Inc()

	if c.logger != nil {
		c.logger.Info("task failed, retries exhausted",
			zap.String("task_id", taskID),
			zap.String("error", errMsg),
		)
	}

	if workflowID != "" {
		c.applyFailurePolicy(workflowID, taskID, policy)
	}

	return nil
}

// applyFailurePolicy applies the workflow-level failure policy after a task fails.
func (c *Coordinator) applyFailurePolicy(workflowID, failedTaskID string, policy scheduler.FailurePolicy) {
	switch policy {
	case scheduler.FailurePolicy_FAIL_FAST:
		cancelled, err := c.state.CancelWorkflowTasks(workflowID)
		if err != nil {
			if c.logger != nil {
				c.logger.Error("FAIL_FAST: failed to cancel workflow tasks",
					zap.String("workflow_id", workflowID),
					zap.Error(err),
				)
			}
			return
		}
		// Remove cancelled tasks from in-memory ready queue
		cancelSet := make(map[string]bool, len(cancelled))
		for _, id := range cancelled {
			cancelSet[id] = true
		}
		c.mu.Lock()
		filtered := c.readyQueue[:0]
		for _, id := range c.readyQueue {
			if !cancelSet[id] {
				filtered = append(filtered, id)
			}
		}
		c.readyQueue = filtered
		c.mu.Unlock()

		if err := c.state.UpdateWorkflowState(workflowID, scheduler.WorkflowState_WORKFLOW_FAILED); err != nil && c.logger != nil {
			c.logger.Error("failed to update workflow state to FAILED", zap.Error(err))
		}

	case scheduler.FailurePolicy_SKIP_DOWNSTREAM, scheduler.FailurePolicy_CONTINUE_INDEPENDENT:
		// CONTINUE_INDEPENDENT is identical to SKIP_DOWNSTREAM: both skip transitive
		// downstream tasks of the failed task and allow independent branches to continue.
		skipped, err := c.state.SkipDownstreamTasks(failedTaskID)
		if err != nil && c.logger != nil {
			c.logger.Error("SKIP_DOWNSTREAM: failed to skip downstream tasks", zap.Error(err))
		}
		if len(skipped) > 0 {
			skipSet := make(map[string]bool, len(skipped))
			for _, id := range skipped {
				skipSet[id] = true
			}
			c.mu.Lock()
			filtered := c.readyQueue[:0]
			for _, id := range c.readyQueue {
				if !skipSet[id] {
					filtered = append(filtered, id)
				}
			}
			c.readyQueue = filtered
			c.mu.Unlock()
		}
		c.checkWorkflowCompletion(workflowID)
	}
}

// checkWorkflowCompletion updates workflow state to COMPLETE or FAILED when all tasks are terminal.
func (c *Coordinator) checkWorkflowCompletion(workflowID string) {
	done, err := c.state.IsWorkflowComplete(workflowID)
	if err != nil || !done {
		return
	}

	// Determine final state: FAILED if any task failed, otherwise COMPLETE
	taskStatuses, err := c.state.GetTaskStatusesForWorkflow(workflowID)
	if err != nil {
		return
	}

	finalState := scheduler.WorkflowState_WORKFLOW_COMPLETE
	for _, ts := range taskStatuses {
		if ts.State == scheduler.TaskState_TASK_FAILED {
			finalState = scheduler.WorkflowState_WORKFLOW_FAILED
			break
		}
	}

	if err := c.state.UpdateWorkflowState(workflowID, finalState); err != nil && c.logger != nil {
		c.logger.Error("failed to update workflow state", zap.Error(err))
	}

	if c.logger != nil {
		c.logger.Info("workflow complete",
			zap.String("workflow_id", workflowID),
			zap.String("final_state", finalState.String()),
		)
	}
}

// CancelWorkflow cancels a running workflow and all non-terminal tasks.
func (c *Coordinator) CancelWorkflow(workflowID string) error {
	cancelled, err := c.state.CancelWorkflowTasks(workflowID)
	if err != nil {
		return err
	}

	cancelSet := make(map[string]bool, len(cancelled))
	for _, id := range cancelled {
		cancelSet[id] = true
	}

	c.mu.Lock()
	filtered := c.readyQueue[:0]
	for _, id := range c.readyQueue {
		if !cancelSet[id] {
			filtered = append(filtered, id)
		}
	}
	c.readyQueue = filtered
	c.mu.Unlock()

	return c.state.UpdateWorkflowState(workflowID, scheduler.WorkflowState_WORKFLOW_CANCELLED)
}

// taskWorkflowID returns the workflow ID for a task from the in-memory cache.
func (c *Coordinator) taskWorkflowID(taskID string) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.taskWorkflowMap[taskID]
}

// HandleDeadWorker requeues all running tasks from a dead worker
func (c *Coordinator) HandleDeadWorker(workerID string) error {
	c.metricsMu.Lock()
	c.deadWorkersHandled++
	c.lastDeadWorkerAtMs = time.Now().UnixMilli()
	c.metricsMu.Unlock()

	c.mu.Lock()

	var tasksToRequeue []string
	for taskID, wID := range c.runningTasks {
		if wID == workerID {
			tasksToRequeue = append(tasksToRequeue, taskID)
		}
	}

	for _, taskID := range tasksToRequeue {
		delete(c.runningTasks, taskID)
		delete(c.assignedTasks, taskID)
	}

	c.mu.Unlock()

	for _, taskID := range tasksToRequeue {
		execID := uuid.New().String()
		if err := c.state.RequeueTask(taskID, execID); err != nil {
			if c.logger != nil {
				c.logger.Error("failed to requeue task",
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

	c.heartbeat.RemoveWorker(workerID)

	metrics.WorkerDisconnectsTotal.Inc()
	if len(tasksToRequeue) > 0 {
		metrics.TasksRequeuedTotal.WithLabelValues("dead_worker").Add(float64(len(tasksToRequeue)))
	}

	if c.logger != nil {
		c.logger.Warn("worker handled as dead",
			zap.String("worker_id", workerID),
			zap.Int("requeued_tasks", len(tasksToRequeue)),
		)
	}

	return nil
}

// UpdateWorkerHeartbeat updates worker heartbeat
func (c *Coordinator) UpdateWorkerHeartbeat(workerID string, timestampMs int64, runningTasks []string) {
	c.heartbeat.UpdateHeartbeat(workerID, timestampMs, runningTasks)
}

// GetReadyQueueSize returns the size of the ready queue
func (c *Coordinator) GetReadyQueueSize() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.readyQueue)
}
