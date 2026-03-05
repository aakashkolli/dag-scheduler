package store

import (
	"bytes"
	"encoding/binary"
	"github.com/aakashkolli/dag-scheduler/internal/proto/scheduler"
)

// ── Serialization helpers ─────────────────────────────────────────────────────

func (s *StateStore) marshalTaskRecord(r *TaskStateRecord) ([]byte, error) {
	var buf bytes.Buffer

	writeString := func(str string) error {
		if err := binary.Write(&buf, binary.LittleEndian, int32(len(str))); err != nil {
			return err
		}
		_, err := buf.WriteString(str)
		return err
	}

	if err := writeString(r.TaskID); err != nil {
		return nil, err
	}
	if err := writeString(r.WorkflowID); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, int32(r.State)); err != nil {
		return nil, err
	}
	if err := writeString(r.ExecutionID); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, r.AttemptCount); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, r.LastUpdated); err != nil {
		return nil, err
	}
	if err := writeString(r.Output); err != nil {
		return nil, err
	}
	if err := writeString(r.Error); err != nil {
		return nil, err
	}
	if err := binary.Write(&buf, binary.LittleEndian, r.InDegree); err != nil {
		return nil, err
	}
	// DownstreamTaskIDs: count + each ID
	if err := binary.Write(&buf, binary.LittleEndian, int32(len(r.DownstreamTaskIDs))); err != nil {
		return nil, err
	}
	for _, id := range r.DownstreamTaskIDs {
		if err := writeString(id); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func (s *StateStore) unmarshalTaskRecord(data []byte) (*TaskStateRecord, error) {
	buf := bytes.NewReader(data)

	readString := func() (string, error) {
		var l int32
		if err := binary.Read(buf, binary.LittleEndian, &l); err != nil {
			return "", err
		}
		b := make([]byte, l)
		if _, err := buf.Read(b); err != nil {
			return "", err
		}
		return string(b), nil
	}

	taskID, err := readString()
	if err != nil {
		return nil, err
	}
	workflowID, err := readString()
	if err != nil {
		return nil, err
	}
	var state int32
	if err := binary.Read(buf, binary.LittleEndian, &state); err != nil {
		return nil, err
	}
	executionID, err := readString()
	if err != nil {
		return nil, err
	}
	var attemptCount int32
	if err := binary.Read(buf, binary.LittleEndian, &attemptCount); err != nil {
		return nil, err
	}
	var lastUpdated int64
	if err := binary.Read(buf, binary.LittleEndian, &lastUpdated); err != nil {
		return nil, err
	}
	output, err := readString()
	if err != nil {
		return nil, err
	}
	errStr, err := readString()
	if err != nil {
		return nil, err
	}
	var inDegree int32
	if err := binary.Read(buf, binary.LittleEndian, &inDegree); err != nil {
		return nil, err
	}

	// DownstreamTaskIDs — new field; old records may not have it (handle gracefully)
	var downstreamCount int32
	var downstreamIDs []string
	if err := binary.Read(buf, binary.LittleEndian, &downstreamCount); err == nil {
		downstreamIDs = make([]string, 0, downstreamCount)
		for i := 0; i < int(downstreamCount); i++ {
			id, err := readString()
			if err != nil {
				break
			}
			downstreamIDs = append(downstreamIDs, id)
		}
	}

	return &TaskStateRecord{
		TaskID:            taskID,
		WorkflowID:        workflowID,
		State:             scheduler.TaskState(state),
		ExecutionID:       executionID,
		AttemptCount:      attemptCount,
		LastUpdated:       lastUpdated,
		Output:            output,
		Error:             errStr,
		InDegree:          inDegree,
		DownstreamTaskIDs: downstreamIDs,
	}, nil
}

func (s *StateStore) marshalWorkflowState(r *WorkflowStateRecord) []byte {
	var buf bytes.Buffer

	writeString := func(str string) {
		_ = binary.Write(&buf, binary.LittleEndian, int32(len(str)))
		buf.WriteString(str)
	}

	writeString(r.WorkflowID)
	binary.Write(&buf, binary.LittleEndian, int32(r.State))
	binary.Write(&buf, binary.LittleEndian, r.SubmittedAtMs)
	binary.Write(&buf, binary.LittleEndian, r.CompletedAtMs)

	return buf.Bytes()
}

func (s *StateStore) unmarshalWorkflowState(data []byte) *WorkflowStateRecord {
	buf := bytes.NewReader(data)

	readString := func() string {
		var l int32
		binary.Read(buf, binary.LittleEndian, &l)
		b := make([]byte, l)
		buf.Read(b)
		return string(b)
	}

	workflowID := readString()
	var state int32
	binary.Read(buf, binary.LittleEndian, &state)
	var submittedAt, completedAt int64
	binary.Read(buf, binary.LittleEndian, &submittedAt)
	binary.Read(buf, binary.LittleEndian, &completedAt)

	return &WorkflowStateRecord{
		WorkflowID:    workflowID,
		State:         scheduler.WorkflowState(state),
		SubmittedAtMs: submittedAt,
		CompletedAtMs: completedAt,
	}
}

func (s *StateStore) marshalWorkerRecord(r *WorkerRecord) []byte {
	var buf bytes.Buffer

	writeString := func(str string) {
		binary.Write(&buf, binary.LittleEndian, int32(len(str)))
		buf.WriteString(str)
	}

	writeString(r.WorkerID)
	binary.Write(&buf, binary.LittleEndian, r.LastHeartbeat)
	binary.Write(&buf, binary.LittleEndian, int32(len(r.RunningTaskIDs)))
	for _, taskID := range r.RunningTaskIDs {
		writeString(taskID)
	}
	binary.Write(&buf, binary.LittleEndian, int32(len(r.QueuedTaskIDs)))
	for _, taskID := range r.QueuedTaskIDs {
		writeString(taskID)
	}
	binary.Write(&buf, binary.LittleEndian, r.LastUpdated)

	return buf.Bytes()
}

func (s *StateStore) unmarshalWorkerRecord(data []byte) *WorkerRecord {
	buf := bytes.NewReader(data)

	readString := func() string {
		var l int32
		binary.Read(buf, binary.LittleEndian, &l)
		b := make([]byte, l)
		buf.Read(b)
		return string(b)
	}

	workerID := readString()
	var lastHeartbeat int64
	binary.Read(buf, binary.LittleEndian, &lastHeartbeat)

	var runningLen int32
	binary.Read(buf, binary.LittleEndian, &runningLen)
	runningTaskIDs := make([]string, runningLen)
	for i := range runningTaskIDs {
		runningTaskIDs[i] = readString()
	}

	var queuedLen int32
	binary.Read(buf, binary.LittleEndian, &queuedLen)
	queuedTaskIDs := make([]string, queuedLen)
	for i := range queuedTaskIDs {
		queuedTaskIDs[i] = readString()
	}

	var lastUpdated int64
	binary.Read(buf, binary.LittleEndian, &lastUpdated)

	return &WorkerRecord{
		WorkerID:       workerID,
		LastHeartbeat:  lastHeartbeat,
		RunningTaskIDs: runningTaskIDs,
		QueuedTaskIDs:  queuedTaskIDs,
		LastUpdated:    lastUpdated,
	}
}
