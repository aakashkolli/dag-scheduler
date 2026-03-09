package coordinator

import (
	"context"
	"sync"
	"time"

	"github.com/aakashkolli/dag-scheduler/internal/proto/scheduler"
	"github.com/aakashkolli/dag-scheduler/internal/store"
	"go.uber.org/zap"
)

// Coordinator orchestrates task execution across workers
type Coordinator struct {
	state         *store.StateStore
	heartbeat     *HeartbeatMonitor
	logger        *zap.Logger
	mu            sync.RWMutex
	readyQueue    []string
	runningTasks  map[string]string
	assignedTasks map[string]string

	taskSpecCache         map[string]*scheduler.TaskSpec
	taskWorkflowMap       map[string]string
	workflowFailurePolicy map[string]scheduler.FailurePolicy

	ctx    context.Context
	cancel context.CancelFunc
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

// Start initializes the coordinator and starts background processes
func (c *Coordinator) Start() error {
	go c.heartbeat.Run(c.ctx, func(workerID string) error {
		return nil // dead worker handler — wired in later
	})
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
