package coordinator

import (
	"os"
	"testing"

	"github.com/aakashkolli/dag-scheduler/internal/proto/scheduler"
	"github.com/aakashkolli/dag-scheduler/internal/store"
	"go.uber.org/zap"
)

// newTestCoordinator spins up a coordinator backed by a temp BoltDB.
func newTestCoordinator(t *testing.T) (*Coordinator, func()) {
	t.Helper()
	f, err := os.CreateTemp("", "coord-unit-*.db")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	f.Close()

	st, err := store.New(f.Name())
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	coord := NewCoordinator(st, zap.NewNop())
	if err := coord.Start(); err != nil {
		t.Fatalf("coord.Start: %v", err)
	}
	return coord, func() {
		coord.Stop()
		st.Close()
		os.Remove(f.Name())
	}
}

func TestCoordinator_SubmitAndCompleteLinear(t *testing.T) {
	coord, cleanup := newTestCoordinator(t)
	defer cleanup()

	wf := &scheduler.WorkflowSpec{
		WorkflowId: "linear-1",
		Tasks: []*scheduler.TaskSpec{
			{TaskId: "t1", Command: "echo hi", MaxRetries: 3},
			{TaskId: "t2", Dependencies: []string{"t1"}, Command: "echo bye", MaxRetries: 3},
		},
	}

	if err := coord.SubmitWorkflow(wf); err != nil {
		t.Fatalf("SubmitWorkflow: %v", err)
	}
	if coord.GetReadyQueueSize() != 1 {
		t.Fatalf("expected 1 ready task, got %d", coord.GetReadyQueueSize())
	}

	task1 := coord.GetReadyTask("w1")
	if task1 == nil || task1.TaskId != "t1" {
		t.Fatalf("expected t1, got %v", task1)
	}
	if coord.GetReadyQueueSize() != 0 {
		t.Errorf("expected empty queue while t1 running")
	}

	if err := coord.CompleteTask("t1", task1.ExecutionId, "hi\n"); err != nil {
		t.Fatalf("CompleteTask t1: %v", err)
	}
	if coord.GetReadyQueueSize() != 1 {
		t.Fatalf("expected t2 queued after t1 completion, got %d", coord.GetReadyQueueSize())
	}

	task2 := coord.GetReadyTask("w1")
	if task2 == nil || task2.TaskId != "t2" {
		t.Fatalf("expected t2, got %v", task2)
	}
	if err := coord.CompleteTask("t2", task2.ExecutionId, "bye\n"); err != nil {
		t.Fatalf("CompleteTask t2: %v", err)
	}
	if coord.GetReadyQueueSize() != 0 {
		t.Errorf("expected empty queue after all tasks complete")
	}
}

func TestCoordinator_StaleExecutionRejected(t *testing.T) {
	coord, cleanup := newTestCoordinator(t)
	defer cleanup()

	wf := &scheduler.WorkflowSpec{
		WorkflowId: "stale-exec",
		Tasks:      []*scheduler.TaskSpec{{TaskId: "t1", Command: "echo", MaxRetries: 3}},
	}
	if err := coord.SubmitWorkflow(wf); err != nil {
		t.Fatalf("submit: %v", err)
	}

	task := coord.GetReadyTask("w1")
	if task == nil {
		t.Fatal("expected task")
	}

	err := coord.CompleteTask("t1", "wrong-execution-id", "output")
	if err == nil {
		t.Error("expected error on stale execution_id")
	}
}

func TestCoordinator_IdempotentCompletion(t *testing.T) {
	coord, cleanup := newTestCoordinator(t)
	defer cleanup()

	wf := &scheduler.WorkflowSpec{
		WorkflowId: "idempotent",
		Tasks:      []*scheduler.TaskSpec{{TaskId: "t1", Command: "echo", MaxRetries: 3}},
	}
	coord.SubmitWorkflow(wf)
	task := coord.GetReadyTask("w1")

	// First completion succeeds
	if err := coord.CompleteTask("t1", task.ExecutionId, "out"); err != nil {
		t.Fatalf("first completion: %v", err)
	}
	// Second call with same execution_id is idempotent
	if err := coord.CompleteTask("t1", task.ExecutionId, "out"); err != nil {
		t.Errorf("idempotent completion should not error, got: %v", err)
	}
}

func TestCoordinator_DeadWorkerRequeuesTask(t *testing.T) {
	coord, cleanup := newTestCoordinator(t)
	defer cleanup()

	wf := &scheduler.WorkflowSpec{
		WorkflowId: "dead-worker",
		Tasks:      []*scheduler.TaskSpec{{TaskId: "t1", Command: "echo", MaxRetries: 3}},
	}
	coord.SubmitWorkflow(wf)

	task := coord.GetReadyTask("doomed-worker")
	if task == nil {
		t.Fatal("expected task")
	}
	if coord.GetReadyQueueSize() != 0 {
		t.Errorf("task should be dequeued while running")
	}

	if err := coord.HandleDeadWorker("doomed-worker"); err != nil {
		t.Fatalf("HandleDeadWorker: %v", err)
	}

	// Task must be re-queued with a new execution_id
	if coord.GetReadyQueueSize() != 1 {
		t.Errorf("expected 1 requeued task, got %d", coord.GetReadyQueueSize())
	}

	// Old execution_id is now stale
	newTask := coord.GetReadyTask("survivor")
	if newTask == nil {
		t.Fatal("expected reassigned task")
	}
	if newTask.ExecutionId == task.ExecutionId {
		t.Error("new assignment should have a different execution_id")
	}
}

func TestCoordinator_RetryOnFailure(t *testing.T) {
	coord, cleanup := newTestCoordinator(t)
	defer cleanup()

	wf := &scheduler.WorkflowSpec{
		WorkflowId: "retry-test",
		Tasks:      []*scheduler.TaskSpec{{TaskId: "t1", Command: "exit 1", MaxRetries: 3}},
	}
	coord.SubmitWorkflow(wf)

	task := coord.GetReadyTask("w1")
	if task == nil {
		t.Fatal("expected task")
	}
	// First failure — should requeue (attempt 1 < MaxRetries 3)
	if err := coord.FailTask("t1", task.ExecutionId, "exit 1"); err != nil {
		t.Fatalf("FailTask: %v", err)
	}
	if coord.GetReadyQueueSize() != 1 {
		t.Errorf("expected task requeued after first failure, got queue=%d", coord.GetReadyQueueSize())
	}
}

func TestCoordinator_FailFastPolicy(t *testing.T) {
	coord, cleanup := newTestCoordinator(t)
	defer cleanup()

	wf := &scheduler.WorkflowSpec{
		WorkflowId:    "fail-fast",
		FailurePolicy: scheduler.FailurePolicy_FAIL_FAST,
		Tasks: []*scheduler.TaskSpec{
			{TaskId: "root", Command: "exit 1", MaxRetries: 1},
			{TaskId: "child", Dependencies: []string{"root"}, Command: "echo never", MaxRetries: 3},
		},
	}
	coord.SubmitWorkflow(wf)

	task := coord.GetReadyTask("w1")
	if task == nil {
		t.Fatal("expected root task")
	}
	// MaxRetries=1: one failure exhausts retries
	coord.FailTask("root", task.ExecutionId, "exit 1")

	// child should never become ready
	if coord.GetReadyQueueSize() != 0 {
		t.Errorf("FAIL_FAST: expected 0 ready tasks, got %d", coord.GetReadyQueueSize())
	}
}

func TestCoordinator_CancelWorkflow(t *testing.T) {
	coord, cleanup := newTestCoordinator(t)
	defer cleanup()

	wf := &scheduler.WorkflowSpec{
		WorkflowId: "cancel-me",
		Tasks: []*scheduler.TaskSpec{
			{TaskId: "t1", Command: "sleep 100", MaxRetries: 3},
			{TaskId: "t2", Dependencies: []string{"t1"}, Command: "echo x", MaxRetries: 3},
		},
	}
	coord.SubmitWorkflow(wf)

	if err := coord.CancelWorkflow("cancel-me"); err != nil {
		t.Fatalf("CancelWorkflow: %v", err)
	}
	if coord.GetReadyQueueSize() != 0 {
		t.Errorf("expected empty queue after cancel, got %d", coord.GetReadyQueueSize())
	}
}

func TestCoordinator_SnapshotMetrics(t *testing.T) {
	coord, cleanup := newTestCoordinator(t)
	defer cleanup()

	wf := &scheduler.WorkflowSpec{
		WorkflowId: "metrics-wf",
		Tasks:      []*scheduler.TaskSpec{{TaskId: "t1", Command: "echo", MaxRetries: 3}},
	}
	coord.SubmitWorkflow(wf)

	snap := coord.SnapshotMetrics()
	if snap.ReadyQueueSize != 1 {
		t.Errorf("expected ReadyQueueSize=1, got %d", snap.ReadyQueueSize)
	}

	coord.GetReadyTask("w1")
	snap = coord.SnapshotMetrics()
	if snap.RunningTaskCount != 1 {
		t.Errorf("expected RunningTaskCount=1, got %d", snap.RunningTaskCount)
	}
}

func TestCoordinator_PrepareRecovery(t *testing.T) {
	f, _ := os.CreateTemp("", "recovery-unit-*.db")
	f.Close()
	defer os.Remove(f.Name())

	// First coordinator: submit workflow, assign task, then "crash"
	st1, _ := store.New(f.Name())
	coord1 := NewCoordinator(st1, zap.NewNop())
	coord1.Start()

	wf := &scheduler.WorkflowSpec{
		WorkflowId: "recovery-unit",
		Tasks:      []*scheduler.TaskSpec{{TaskId: "t1", Command: "echo", MaxRetries: 3}},
	}
	coord1.SubmitWorkflow(wf)
	coord1.GetReadyTask("w1") // marks t1 as RUNNING in BoltDB
	coord1.Stop()
	st1.Close()

	// Second coordinator: recover
	st2, _ := store.New(f.Name())
	defer st2.Close()
	coord2 := NewCoordinator(st2, zap.NewNop())

	if err := coord2.PrepareRecovery(); err != nil {
		t.Fatalf("PrepareRecovery: %v", err)
	}

	// t1 should be re-queued
	rec, err := st2.GetTask("t1")
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	if rec.State != scheduler.TaskState_TASK_QUEUED {
		t.Errorf("expected t1 QUEUED after recovery, got %v", rec.State)
	}
	coord2.Stop()
}
