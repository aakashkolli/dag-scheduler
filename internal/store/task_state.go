package store

import (
	"time"

	"github.com/aakashkolli/dag-scheduler/internal/metrics"
	"github.com/aakashkolli/dag-scheduler/internal/proto/scheduler"
	"go.etcd.io/bbolt"
)

// ── Task state ────────────────────────────────────────────────────────────────

// InitializeTasksForWorkflow creates initial PENDING task state records.
// downstream maps task_id -> list of task_ids that depend on it; this is
// stored in each TaskStateRecord to make CompleteTask O(k) not O(n).
func (s *StateStore) InitializeTasksForWorkflow(
	workflowID string,
	tasks []*scheduler.TaskSpec,
	inDegrees map[string]int32,
	downstream map[string][]string,
) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(BucketTaskState))
		for _, task := range tasks {
			record := &TaskStateRecord{
				TaskID:            task.TaskId,
				WorkflowID:        workflowID,
				State:             scheduler.TaskState_TASK_PENDING,
				AttemptCount:      0,
				LastUpdated:       time.Now().UnixMilli(),
				InDegree:          inDegrees[task.TaskId],
				DownstreamTaskIDs: downstream[task.TaskId],
			}
			data, err := s.marshalTaskRecord(record)
			if err != nil {
				return err
			}
			if err := b.Put([]byte(task.TaskId), data); err != nil {
				return err
			}
		}
		return nil
	})
}

// GetTask retrieves a task state record.
func (s *StateStore) GetTask(taskID string) (*TaskStateRecord, error) {
	var result *TaskStateRecord
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(BucketTaskState))
		data := b.Get([]byte(taskID))
		if data == nil {
			return ErrTaskNotFound
		}
		rec, err := s.unmarshalTaskRecord(data)
		if err != nil {
			return err
		}
		result = rec
		return nil
	})
	return result, err
}

// UpdateTaskState updates a task's state atomically.
func (s *StateStore) UpdateTaskState(taskID string, newState scheduler.TaskState) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(BucketTaskState))
		data := b.Get([]byte(taskID))
		if data == nil {
			return ErrTaskNotFound
		}
		record, err := s.unmarshalTaskRecord(data)
		if err != nil {
			return err
		}
		record.State = newState
		record.LastUpdated = time.Now().UnixMilli()
		newData, err := s.marshalTaskRecord(record)
		if err != nil {
			return err
		}
		return b.Put([]byte(taskID), newData)
	})
}

// AssignTask marks a task as RUNNING with an execution ID atomically.
func (s *StateStore) AssignTask(taskID string, executionID string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(BucketTaskState))
		data := b.Get([]byte(taskID))
		if data == nil {
			return ErrTaskNotFound
		}
		record, err := s.unmarshalTaskRecord(data)
		if err != nil {
			return err
		}
		record.State = scheduler.TaskState_TASK_RUNNING
		record.ExecutionID = executionID
		record.LastUpdated = time.Now().UnixMilli()
		newData, err := s.marshalTaskRecord(record)
		if err != nil {
			return err
		}
		return b.Put([]byte(taskID), newData)
	})
}

// CompleteTask marks a task COMPLETE and decrements in-degrees of its downstream
// tasks — all in a single BoltDB transaction.
// Uses stored DownstreamTaskIDs for O(k) performance instead of O(n) ForEach.
// Returns the list of downstream task IDs whose in-degree reached zero (now QUEUED).
func (s *StateStore) CompleteTask(taskID string, executionID string, output string) (downstreamReady []string, err error) {
	txStart := time.Now()
	err = s.db.Update(func(tx *bbolt.Tx) error {
		bTasks := tx.Bucket([]byte(BucketTaskState))

		data := bTasks.Get([]byte(taskID))
		if data == nil {
			return ErrTaskNotFound
		}
		task, err := s.unmarshalTaskRecord(data)
		if err != nil {
			return err
		}

		// Idempotency: wrong execution_id -> reject
		if task.ExecutionID != executionID {
			return ErrStaleExecution
		}
		// Idempotency: already complete -> no-op (downstreamReady stays nil)
		if task.State == scheduler.TaskState_TASK_COMPLETE {
			return nil
		}

		task.State = scheduler.TaskState_TASK_COMPLETE
		task.Output = output
		task.LastUpdated = time.Now().UnixMilli()
		newData, err := s.marshalTaskRecord(task)
		if err != nil {
			return err
		}
		if err := bTasks.Put([]byte(taskID), newData); err != nil {
			return err
		}

		// Decrement in-degree of each downstream task in the same transaction.
		downstreamReady = []string{}
		for _, downstreamID := range task.DownstreamTaskIDs {
			dsData := bTasks.Get([]byte(downstreamID))
			if dsData == nil {
				continue
			}
			ds, err := s.unmarshalTaskRecord(dsData)
			if err != nil {
				continue
			}
			// Skip tasks that are already in a terminal or active state
			if ds.State != scheduler.TaskState_TASK_PENDING {
				continue
			}
			ds.InDegree--
			if ds.InDegree < 0 {
				ds.InDegree = 0
			}
			if ds.InDegree == 0 {
				ds.State = scheduler.TaskState_TASK_QUEUED
				downstreamReady = append(downstreamReady, downstreamID)
			}
			ds.LastUpdated = time.Now().UnixMilli()
			newDsData, err := s.marshalTaskRecord(ds)
			if err != nil {
				return err
			}
			if err := bTasks.Put([]byte(downstreamID), newDsData); err != nil {
				return err
			}
		}

		return nil
	})
	metrics.BoltDBTxDurationMs.Observe(float64(time.Since(txStart).Milliseconds()))
	return
}

// FailTask marks a task as FAILED with the given error message and attempt count.
func (s *StateStore) FailTask(taskID string, errMsg string, attemptCount int32) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(BucketTaskState))
		data := b.Get([]byte(taskID))
		if data == nil {
			return ErrTaskNotFound
		}
		record, err := s.unmarshalTaskRecord(data)
		if err != nil {
			return err
		}
		record.State = scheduler.TaskState_TASK_FAILED
		record.Error = errMsg
		record.AttemptCount = attemptCount
		record.LastUpdated = time.Now().UnixMilli()
		newData, err := s.marshalTaskRecord(record)
		if err != nil {
			return err
		}
		return b.Put([]byte(taskID), newData)
	})
}

// RequeueTask marks a task back as QUEUED with a new execution ID.
func (s *StateStore) RequeueTask(taskID string, newExecutionID string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(BucketTaskState))
		data := b.Get([]byte(taskID))
		if data == nil {
			return ErrTaskNotFound
		}
		record, err := s.unmarshalTaskRecord(data)
		if err != nil {
			return err
		}
		record.State = scheduler.TaskState_TASK_QUEUED
		record.ExecutionID = newExecutionID
		record.LastUpdated = time.Now().UnixMilli()
		newData, err := s.marshalTaskRecord(record)
		if err != nil {
			return err
		}
		return b.Put([]byte(taskID), newData)
	})
}

// GetTasksInState returns all task IDs currently in the given state.
func (s *StateStore) GetTasksInState(state scheduler.TaskState) ([]string, error) {
	var result []string
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(BucketTaskState))
		return b.ForEach(func(k, v []byte) error {
			record, err := s.unmarshalTaskRecord(v)
			if err != nil {
				return err
			}
			if record.State == state {
				result = append(result, record.TaskID)
			}
			return nil
		})
	})
	return result, err
}

// GetTasksForWorkflow returns all task IDs belonging to a workflow.
func (s *StateStore) GetTasksForWorkflow(workflowID string) ([]string, error) {
	var result []string
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(BucketTaskState))
		return b.ForEach(func(k, v []byte) error {
			record, err := s.unmarshalTaskRecord(v)
			if err != nil {
				return err
			}
			if record.WorkflowID == workflowID {
				result = append(result, record.TaskID)
			}
			return nil
		})
	})
	return result, err
}

// GetTaskStatusesForWorkflow returns task status records for all tasks in a workflow.
func (s *StateStore) GetTaskStatusesForWorkflow(workflowID string) ([]*scheduler.TaskStatus, error) {
	var result []*scheduler.TaskStatus
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(BucketTaskState))
		return b.ForEach(func(k, v []byte) error {
			record, err := s.unmarshalTaskRecord(v)
			if err != nil {
				return err
			}
			if record.WorkflowID != workflowID {
				return nil
			}
			result = append(result, &scheduler.TaskStatus{
				TaskId:       record.TaskID,
				State:        record.State,
				AttemptCount: record.AttemptCount,
				Output:       record.Output,
				Error:        record.Error,
			})
			return nil
		})
	})
	return result, err
}

// CancelWorkflowTasks marks all non-terminal tasks in a workflow as SKIPPED.
// Returns the list of task IDs that were cancelled (removed from active queues).
func (s *StateStore) CancelWorkflowTasks(workflowID string) ([]string, error) {
	var cancelled []string
	err := s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(BucketTaskState))
		return b.ForEach(func(k, v []byte) error {
			record, err := s.unmarshalTaskRecord(v)
			if err != nil {
				return err
			}
			if record.WorkflowID != workflowID {
				return nil
			}
			// Terminal states: COMPLETE, FAILED, SKIPPED
			switch record.State {
			case scheduler.TaskState_TASK_COMPLETE,
				scheduler.TaskState_TASK_FAILED,
				scheduler.TaskState_TASK_SKIPPED:
				return nil
			}
			record.State = scheduler.TaskState_TASK_SKIPPED
			record.LastUpdated = time.Now().UnixMilli()
			newData, err := s.marshalTaskRecord(record)
			if err != nil {
				return err
			}
			cancelled = append(cancelled, record.TaskID)
			return b.Put(k, newData)
		})
	})
	return cancelled, err
}

// SkipDownstreamTasks marks all downstream tasks of failedTaskID as SKIPPED.
// Only skips PENDING and QUEUED tasks — RUNNING tasks are left to report results.
func (s *StateStore) SkipDownstreamTasks(failedTaskID string) ([]string, error) {
	var skipped []string

	// Collect the downstream IDs from the failed task's record
	failedTask, err := s.GetTask(failedTaskID)
	if err != nil {
		return nil, err
	}

	// BFS over the downstream graph using stored DownstreamTaskIDs
	visited := make(map[string]bool)
	queue := append([]string{}, failedTask.DownstreamTaskIDs...)

	for len(queue) > 0 {
		taskID := queue[0]
		queue = queue[1:]
		if visited[taskID] {
			continue
		}
		visited[taskID] = true

		task, err := s.GetTask(taskID)
		if err != nil {
			continue
		}
		// Enqueue this task's downstream for further skipping
		queue = append(queue, task.DownstreamTaskIDs...)

		// Only skip non-terminal tasks
		switch task.State {
		case scheduler.TaskState_TASK_COMPLETE,
			scheduler.TaskState_TASK_FAILED,
			scheduler.TaskState_TASK_SKIPPED:
			continue
		}

		if err := s.UpdateTaskState(taskID, scheduler.TaskState_TASK_SKIPPED); err != nil {
			continue
		}
		skipped = append(skipped, taskID)
	}

	return skipped, nil
}

// IsWorkflowComplete returns true when every task in the workflow is in a terminal state.
func (s *StateStore) IsWorkflowComplete(workflowID string) (bool, error) {
	complete := true
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(BucketTaskState))
		return b.ForEach(func(k, v []byte) error {
			record, err := s.unmarshalTaskRecord(v)
			if err != nil {
				return err
			}
			if record.WorkflowID != workflowID {
				return nil
			}
			switch record.State {
			case scheduler.TaskState_TASK_COMPLETE,
				scheduler.TaskState_TASK_FAILED,
				scheduler.TaskState_TASK_SKIPPED:
				// terminal
			default:
				complete = false
			}
			return nil
		})
	})
	return complete, err
}
