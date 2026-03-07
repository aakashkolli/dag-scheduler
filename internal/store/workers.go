package store

import (
	"go.etcd.io/bbolt"
)

// ── Worker records ────────────────────────────────────────────────────────────

func (s *StateStore) StoreWorker(worker *WorkerRecord) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(BucketWorkers))
		return b.Put([]byte(worker.WorkerID), s.marshalWorkerRecord(worker))
	})
}

func (s *StateStore) GetWorker(workerID string) (*WorkerRecord, error) {
	var result *WorkerRecord
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(BucketWorkers))
		data := b.Get([]byte(workerID))
		if data == nil {
			return ErrNotFound
		}
		result = s.unmarshalWorkerRecord(data)
		return nil
	})
	return result, err
}

func (s *StateStore) RemoveWorker(workerID string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket([]byte(BucketWorkers)).Delete([]byte(workerID))
	})
}

func (s *StateStore) GetAllWorkers() ([]*WorkerRecord, error) {
	var result []*WorkerRecord
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(BucketWorkers))
		return b.ForEach(func(k, v []byte) error {
			result = append(result, s.unmarshalWorkerRecord(v))
			return nil
		})
	})
	return result, err
}

// GetAllWorkflowStates returns all workflow state records.
func (s *StateStore) GetAllWorkflowStates() ([]*WorkflowStateRecord, error) {
	var result []*WorkflowStateRecord
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(BucketWorkflowState))
		return b.ForEach(func(k, v []byte) error {
			result = append(result, s.unmarshalWorkflowState(v))
			return nil
		})
	})
	return result, err
}
