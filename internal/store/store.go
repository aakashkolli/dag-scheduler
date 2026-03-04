package store

import (
	"errors"
	"sync"
	"time"

	"github.com/aakashkolli/dag-scheduler/internal/proto/scheduler"
	"go.etcd.io/bbolt"
)

// Bucket names
const (
	BucketWorkflows     = "workflows"
	BucketTaskState     = "task_state"
	BucketWorkers       = "workers"
	BucketMeta          = "meta"
	BucketWorkflowState = "workflow_state" // workflow-level state + timestamps
)

var (
	ErrNotFound         = errors.New("not found")
	ErrStaleExecution   = errors.New("stale execution_id")
	ErrInvalidDAG       = errors.New("invalid DAG")
	ErrTaskNotFound     = errors.New("task not found")
	ErrWorkflowNotFound = errors.New("workflow not found")
)

// TaskStateRecord stores task state in BoltDB.
// DownstreamTaskIDs is populated at workflow init time so CompleteTask avoids
// an O(n) ForEach scan to find which tasks to unblock.
type TaskStateRecord struct {
	TaskID            string
	WorkflowID        string
	State             scheduler.TaskState
	ExecutionID       string
	AttemptCount      int32
	LastUpdated       int64 // unix ms
	Output            string
	Error             string
	InDegree          int32
	DownstreamTaskIDs []string // tasks that depend on this task (pre-computed at init)
}

// WorkflowStateRecord tracks workflow-level state and timing.
type WorkflowStateRecord struct {
	WorkflowID    string
	State         scheduler.WorkflowState
	SubmittedAtMs int64
	CompletedAtMs int64
}

// WorkerRecord stores worker state.
type WorkerRecord struct {
	WorkerID       string
	LastHeartbeat  int64 // unix ms
	RunningTaskIDs []string
	QueuedTaskIDs  []string
	LastUpdated    int64 // unix ms
}

// StateStore manages all persistent state via BoltDB.
type StateStore struct {
	db *bbolt.DB
	mu sync.RWMutex
}

// New creates a new StateStore.
func New(dbPath string) (*StateStore, error) {
	db, err := bbolt.Open(dbPath, 0600, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		buckets := []string{BucketWorkflows, BucketTaskState, BucketWorkers, BucketMeta, BucketWorkflowState}
		for _, name := range buckets {
			if _, err := tx.CreateBucketIfNotExists([]byte(name)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}

	return &StateStore{db: db}, nil
}

// Close closes the database.
func (s *StateStore) Close() error {
	return s.db.Close()
}
