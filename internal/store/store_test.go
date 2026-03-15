package store

import (
	"os"
	"testing"

	"github.com/aakashkolli/dag-scheduler/internal/proto/scheduler"
)

// newTestStore creates a StateStore backed by a temp file and returns a cleanup func.
func newTestStore(t *testing.T) (*StateStore, func()) {
	t.Helper()
	f, err := os.CreateTemp("", "store-unit-*.db")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	f.Close()
	st, err := New(f.Name())
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	return st, func() {
		st.Close()
		os.Remove(f.Name())
	}
}

func initWorkflow(t *testing.T, st *StateStore, wfID string, tasks []*scheduler.TaskSpec,
	inDeg map[string]int32, downstream map[string][]string) {
	t.Helper()
	wf := &scheduler.WorkflowSpec{WorkflowId: wfID, Tasks: tasks}
	if err := st.StoreWorkflow(wf); err != nil {
		t.Fatalf("StoreWorkflow: %v", err)
	}
	if err := st.InitWorkflowState(wfID); err != nil {
		t.Fatalf("InitWorkflowState: %v", err)
	}
	if err := st.InitializeTasksForWorkflow(wfID, tasks, inDeg, downstream); err != nil {
		t.Fatalf("InitializeTasksForWorkflow: %v", err)
	}
}

func TestStateStore_WorkflowRoundTrip(t *testing.T) {
	st, cleanup := newTestStore(t)
	defer cleanup()

	wf := &scheduler.WorkflowSpec{
		WorkflowId:    "wf-1",
		FailurePolicy: scheduler.FailurePolicy_SKIP_DOWNSTREAM,
		Tasks:         []*scheduler.TaskSpec{{TaskId: "t1", Command: "echo"}},
	}
	if err := st.StoreWorkflow(wf); err != nil {
		t.Fatalf("StoreWorkflow: %v", err)
	}

	got, err := st.GetWorkflow("wf-1")
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if got.WorkflowId != "wf-1" {
		t.Errorf("expected wf-1, got %s", got.WorkflowId)
	}
	if got.FailurePolicy != scheduler.FailurePolicy_SKIP_DOWNSTREAM {
		t.Errorf("unexpected failure policy: %v", got.FailurePolicy)
	}
}

func TestStateStore_GetWorkflow_NotFound(t *testing.T) {
	st, cleanup := newTestStore(t)
	defer cleanup()

	_, err := st.GetWorkflow("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent workflow")
	}
}

func TestStateStore_TaskStateTransitions(t *testing.T) {
	st, cleanup := newTestStore(t)
	defer cleanup()

	tasks := []*scheduler.TaskSpec{
		{TaskId: "t1"},
		{TaskId: "t2", Dependencies: []string{"t1"}},
	}
	initWorkflow(t, st, "wf-trans", tasks,
		map[string]int32{"t1": 0, "t2": 1},
		map[string][]string{"t1": {"t2"}, "t2": {}},
	)

	// Initial state: PENDING
	rec, _ := st.GetTask("t1")
	if rec.State != scheduler.TaskState_TASK_PENDING {
		t.Errorf("expected PENDING, got %v", rec.State)
	}

	// Assign → RUNNING
	if err := st.AssignTask("t1", "exec-1"); err != nil {
		t.Fatalf("AssignTask: %v", err)
	}
	rec, _ = st.GetTask("t1")
	if rec.State != scheduler.TaskState_TASK_RUNNING {
		t.Errorf("expected RUNNING, got %v", rec.State)
	}
	if rec.ExecutionID != "exec-1" {
		t.Errorf("expected exec-1, got %s", rec.ExecutionID)
	}

	// Complete → t2 becomes QUEUED
	ready, err := st.CompleteTask("t1", "exec-1", "output")
	if err != nil {
		t.Fatalf("CompleteTask: %v", err)
	}
	if len(ready) != 1 || ready[0] != "t2" {
		t.Errorf("expected [t2] ready, got %v", ready)
	}
	t2, _ := st.GetTask("t2")
	if t2.State != scheduler.TaskState_TASK_QUEUED {
		t.Errorf("expected t2 QUEUED, got %v", t2.State)
	}
}

func TestStateStore_CompleteTask_Idempotent(t *testing.T) {
	st, cleanup := newTestStore(t)
	defer cleanup()

	tasks := []*scheduler.TaskSpec{{TaskId: "t1"}}
	initWorkflow(t, st, "idem", tasks,
		map[string]int32{"t1": 0},
		map[string][]string{"t1": {}},
	)
	st.AssignTask("t1", "exec-1")
	st.CompleteTask("t1", "exec-1", "first")

	// Same execution_id: idempotent, no error
	_, err := st.CompleteTask("t1", "exec-1", "second")
	if err != nil {
		t.Errorf("idempotent complete should not error, got %v", err)
	}

	// Different execution_id: stale, should fail
	_, err = st.CompleteTask("t1", "stale-exec", "third")
	if err == nil {
		t.Error("expected stale execution error")
	}
}

func TestStateStore_RequeueTask(t *testing.T) {
	st, cleanup := newTestStore(t)
	defer cleanup()

	tasks := []*scheduler.TaskSpec{{TaskId: "t1"}}
	initWorkflow(t, st, "requeue", tasks,
		map[string]int32{"t1": 0},
		map[string][]string{"t1": {}},
	)
	st.AssignTask("t1", "exec-1")

	if err := st.RequeueTask("t1", "exec-2"); err != nil {
		t.Fatalf("RequeueTask: %v", err)
	}

	rec, _ := st.GetTask("t1")
	if rec.State != scheduler.TaskState_TASK_QUEUED {
		t.Errorf("expected QUEUED after requeue, got %v", rec.State)
	}
	if rec.ExecutionID != "exec-2" {
		t.Errorf("expected exec-2, got %s", rec.ExecutionID)
	}
}

func TestStateStore_FailTask(t *testing.T) {
	st, cleanup := newTestStore(t)
	defer cleanup()

	tasks := []*scheduler.TaskSpec{{TaskId: "t1"}}
	initWorkflow(t, st, "fail-task", tasks,
		map[string]int32{"t1": 0},
		map[string][]string{"t1": {}},
	)
	st.AssignTask("t1", "exec-1")

	if err := st.FailTask("t1", "exit 1", 1); err != nil {
		t.Fatalf("FailTask: %v", err)
	}

	rec, _ := st.GetTask("t1")
	if rec.State != scheduler.TaskState_TASK_FAILED {
		t.Errorf("expected FAILED, got %v", rec.State)
	}
	if rec.Error != "exit 1" {
		t.Errorf("expected error 'exit 1', got '%s'", rec.Error)
	}
}

func TestStateStore_GetTasksInState(t *testing.T) {
	st, cleanup := newTestStore(t)
	defer cleanup()

	tasks := []*scheduler.TaskSpec{{TaskId: "t1"}, {TaskId: "t2"}}
	initWorkflow(t, st, "state-query", tasks,
		map[string]int32{"t1": 0, "t2": 0},
		map[string][]string{"t1": {}, "t2": {}},
	)

	pending, err := st.GetTasksInState(scheduler.TaskState_TASK_PENDING)
	if err != nil {
		t.Fatalf("GetTasksInState: %v", err)
	}
	if len(pending) != 2 {
		t.Errorf("expected 2 pending tasks, got %d", len(pending))
	}

	st.AssignTask("t1", "exec-1")
	running, _ := st.GetTasksInState(scheduler.TaskState_TASK_RUNNING)
	if len(running) != 1 || running[0] != "t1" {
		t.Errorf("expected [t1] running, got %v", running)
	}
}

func TestStateStore_WorkflowStateTimestamps(t *testing.T) {
	st, cleanup := newTestStore(t)
	defer cleanup()

	if err := st.InitWorkflowState("ts-wf"); err != nil {
		t.Fatalf("InitWorkflowState: %v", err)
	}

	rec, err := st.GetWorkflowState("ts-wf")
	if err != nil {
		t.Fatalf("GetWorkflowState: %v", err)
	}
	if rec.SubmittedAtMs == 0 {
		t.Error("expected non-zero SubmittedAtMs")
	}
	if rec.CompletedAtMs != 0 {
		t.Error("expected zero CompletedAtMs while running")
	}

	st.UpdateWorkflowState("ts-wf", scheduler.WorkflowState_WORKFLOW_COMPLETE)
	rec, _ = st.GetWorkflowState("ts-wf")
	if rec.CompletedAtMs == 0 {
		t.Error("expected non-zero CompletedAtMs after completion")
	}
}

func TestStateStore_CancelWorkflowTasks(t *testing.T) {
	st, cleanup := newTestStore(t)
	defer cleanup()

	tasks := []*scheduler.TaskSpec{
		{TaskId: "t1"},
		{TaskId: "t2", Dependencies: []string{"t1"}},
	}
	initWorkflow(t, st, "cancel-wf", tasks,
		map[string]int32{"t1": 0, "t2": 1},
		map[string][]string{"t1": {"t2"}, "t2": {}},
	)

	cancelled, err := st.CancelWorkflowTasks("cancel-wf")
	if err != nil {
		t.Fatalf("CancelWorkflowTasks: %v", err)
	}
	if len(cancelled) != 2 {
		t.Errorf("expected 2 cancelled tasks, got %d: %v", len(cancelled), cancelled)
	}

	for _, id := range []string{"t1", "t2"} {
		rec, _ := st.GetTask(id)
		if rec.State != scheduler.TaskState_TASK_SKIPPED {
			t.Errorf("expected %s SKIPPED, got %v", id, rec.State)
		}
	}
}

func TestStateStore_SkipDownstreamTasks(t *testing.T) {
	st, cleanup := newTestStore(t)
	defer cleanup()

	tasks := []*scheduler.TaskSpec{
		{TaskId: "root"},
		{TaskId: "child1", Dependencies: []string{"root"}},
		{TaskId: "child2", Dependencies: []string{"child1"}},
		{TaskId: "sibling"}, // no dependency on root, should NOT be skipped
	}
	initWorkflow(t, st, "skip-wf", tasks,
		map[string]int32{"root": 0, "child1": 1, "child2": 1, "sibling": 0},
		map[string][]string{
			"root":    {"child1"},
			"child1":  {"child2"},
			"child2":  {},
			"sibling": {},
		},
	)
	st.AssignTask("root", "exec-root")
	st.FailTask("root", "failed", 1)

	skipped, err := st.SkipDownstreamTasks("root")
	if err != nil {
		t.Fatalf("SkipDownstreamTasks: %v", err)
	}
	if len(skipped) != 2 {
		t.Errorf("expected 2 skipped, got %d: %v", len(skipped), skipped)
	}

	// sibling must remain PENDING
	sib, _ := st.GetTask("sibling")
	if sib.State != scheduler.TaskState_TASK_PENDING {
		t.Errorf("sibling should remain PENDING, got %v", sib.State)
	}
}

func TestStateStore_IsWorkflowComplete(t *testing.T) {
	st, cleanup := newTestStore(t)
	defer cleanup()

	tasks := []*scheduler.TaskSpec{{TaskId: "t1"}}
	initWorkflow(t, st, "complete-check", tasks,
		map[string]int32{"t1": 0},
		map[string][]string{"t1": {}},
	)

	done, _ := st.IsWorkflowComplete("complete-check")
	if done {
		t.Error("workflow should not be complete with PENDING task")
	}

	st.AssignTask("t1", "exec-1")
	st.CompleteTask("t1", "exec-1", "output")

	done, _ = st.IsWorkflowComplete("complete-check")
	if !done {
		t.Error("workflow should be complete after all tasks complete")
	}
}

func TestStateStore_Serialization_RoundTrip(t *testing.T) {
	st, cleanup := newTestStore(t)
	defer cleanup()

	original := &TaskStateRecord{
		TaskID:            "task-1",
		WorkflowID:        "wf-1",
		State:             scheduler.TaskState_TASK_RUNNING,
		ExecutionID:       "exec-abc-123",
		AttemptCount:      2,
		LastUpdated:       1700000000000,
		Output:            "hello world",
		Error:             "some error",
		InDegree:          3,
		DownstreamTaskIDs: []string{"t2", "t3", "t4"},
	}

	data, err := st.marshalTaskRecord(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	restored, err := st.unmarshalTaskRecord(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if restored.TaskID != original.TaskID {
		t.Errorf("TaskID: want %s, got %s", original.TaskID, restored.TaskID)
	}
	if restored.ExecutionID != original.ExecutionID {
		t.Errorf("ExecutionID: want %s, got %s", original.ExecutionID, restored.ExecutionID)
	}
	if restored.AttemptCount != original.AttemptCount {
		t.Errorf("AttemptCount: want %d, got %d", original.AttemptCount, restored.AttemptCount)
	}
	if len(restored.DownstreamTaskIDs) != 3 {
		t.Errorf("DownstreamTaskIDs: want 3, got %d", len(restored.DownstreamTaskIDs))
	}
}
