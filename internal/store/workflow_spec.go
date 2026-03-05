package store

import (
	"github.com/aakashkolli/dag-scheduler/internal/proto/scheduler"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
)

// ── Workflow spec ─────────────────────────────────────────────────────────────

// StoreWorkflow persists a workflow spec atomically.
func (s *StateStore) StoreWorkflow(workflow *scheduler.WorkflowSpec) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(BucketWorkflows))
		data, err := proto.Marshal(workflow)
		if err != nil {
			return err
		}
		return b.Put([]byte(workflow.WorkflowId), data)
	})
}

// GetWorkflow retrieves a workflow spec.
func (s *StateStore) GetWorkflow(workflowID string) (*scheduler.WorkflowSpec, error) {
	var result *scheduler.WorkflowSpec
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(BucketWorkflows))
		data := b.Get([]byte(workflowID))
		if data == nil {
			return ErrWorkflowNotFound
		}
		wf := &scheduler.WorkflowSpec{}
		if err := proto.Unmarshal(data, wf); err != nil {
			return err
		}
		result = wf
		return nil
	})
	return result, err
}

// getWorkflowInTx retrieves a workflow spec inside an existing transaction.
func (s *StateStore) getWorkflowInTx(tx *bbolt.Tx, workflowID string) (*scheduler.WorkflowSpec, error) {
	b := tx.Bucket([]byte(BucketWorkflows))
	data := b.Get([]byte(workflowID))
	if data == nil {
		return nil, ErrWorkflowNotFound
	}
	wf := &scheduler.WorkflowSpec{}
	if err := proto.Unmarshal(data, wf); err != nil {
		return nil, err
	}
	return wf, nil
}
