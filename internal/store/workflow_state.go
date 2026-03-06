package store

import (
	"github.com/aakashkolli/dag-scheduler/internal/proto/scheduler"
	"go.etcd.io/bbolt"
	"time"
)

// ── Workflow state ────────────────────────────────────────────────────────────

// InitWorkflowState creates the initial WORKFLOW_RUNNING state record.
func (s *StateStore) InitWorkflowState(workflowID string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(BucketWorkflowState))
		rec := &WorkflowStateRecord{
			WorkflowID:    workflowID,
			State:         scheduler.WorkflowState_WORKFLOW_RUNNING,
			SubmittedAtMs: time.Now().UnixMilli(),
		}
		return b.Put([]byte(workflowID), s.marshalWorkflowState(rec))
	})
}

// UpdateWorkflowState sets the workflow state atomically.
func (s *StateStore) UpdateWorkflowState(workflowID string, state scheduler.WorkflowState) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(BucketWorkflowState))
		data := b.Get([]byte(workflowID))
		var rec *WorkflowStateRecord
		if data != nil {
			rec = s.unmarshalWorkflowState(data)
		} else {
			rec = &WorkflowStateRecord{WorkflowID: workflowID, SubmittedAtMs: time.Now().UnixMilli()}
		}
		rec.State = state
		if state == scheduler.WorkflowState_WORKFLOW_COMPLETE ||
			state == scheduler.WorkflowState_WORKFLOW_FAILED ||
			state == scheduler.WorkflowState_WORKFLOW_CANCELLED {
			rec.CompletedAtMs = time.Now().UnixMilli()
		}
		return b.Put([]byte(workflowID), s.marshalWorkflowState(rec))
	})
}

// GetWorkflowState returns the current state record for a workflow.
func (s *StateStore) GetWorkflowState(workflowID string) (*WorkflowStateRecord, error) {
	var result *WorkflowStateRecord
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(BucketWorkflowState))
		data := b.Get([]byte(workflowID))
		if data == nil {
			return ErrWorkflowNotFound
		}
		result = s.unmarshalWorkflowState(data)
		return nil
	})
	return result, err
}
