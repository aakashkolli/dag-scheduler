package store

import (
	"time"

	"github.com/aakashkolli/dag-scheduler/internal/metrics"
	"github.com/aakashkolli/dag-scheduler/internal/proto/scheduler"
	"go.etcd.io/bbolt"
)

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

func (s *StateStore) AssignTask(taskID, executionID string) error {
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

func (s *StateStore) CompleteTask(taskID, executionID, output string) ([]string, error) {
	var downstreamReady []string
	start := time.Now()
	err := s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(BucketTaskState))
		data := b.Get([]byte(taskID))
		if data == nil {
			return ErrTaskNotFound
		}
		record, err := s.unmarshalTaskRecord(data)
		if err != nil {
			return err
		}
		if record.ExecutionID != executionID {
			return ErrStaleExecution
		}
		record.State = scheduler.TaskState_TASK_COMPLETE
		record.Output = output
		record.LastUpdated = time.Now().UnixMilli()
		newData, err := s.marshalTaskRecord(record)
		if err != nil {
			return err
		}
		if err := b.Put([]byte(taskID), newData); err != nil {
			return err
		}
		for _, downID := range record.DownstreamTaskIDs {
			downData := b.Get([]byte(downID))
			if downData == nil {
				continue
			}
			downRecord, err := s.unmarshalTaskRecord(downData)
			if err != nil {
				continue
			}
			downRecord.InDegree--
			downRecord.LastUpdated = time.Now().UnixMilli()
			if downRecord.InDegree == 0 {
				downRecord.State = scheduler.TaskState_TASK_QUEUED
				downstreamReady = append(downstreamReady, downID)
			}
			newDownData, err := s.marshalTaskRecord(downRecord)
			if err != nil {
				continue
			}
			b.Put([]byte(downID), newDownData)
		}
		return nil
	})
	metrics.BoltDBTxDurationMs.Observe(float64(time.Since(start).Milliseconds()))
	return downstreamReady, err
}

func (s *StateStore) FailTask(taskID, errMsg string, attemptCount int32) error {
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

func (s *StateStore) RequeueTask(taskID, newExecutionID string) error {
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
