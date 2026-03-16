package integration

import (
	"testing"

	"github.com/aakashkolli/dag-scheduler/internal/coordinator"
	"github.com/aakashkolli/dag-scheduler/internal/proto/scheduler"
	"github.com/aakashkolli/dag-scheduler/internal/store"
	"go.uber.org/zap"
)

// TestSimpleWorkflow tests a simple linear workflow
func TestSimpleWorkflow(t *testing.T) {
	// Create a logger for testing
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	// Create state store
	stateStore, err := store.New("/tmp/test_scheduler.db")
	if err != nil {
		t.Fatalf("failed to create state store: %v", err)
	}
	defer stateStore.Close()

	// Create coordinator
	coord := coordinator.NewCoordinator(stateStore, logger)
	if err := coord.Start(); err != nil {
		t.Fatalf("failed to start coordinator: %v", err)
	}

	// Create a simple workflow
	workflow := &scheduler.WorkflowSpec{
		WorkflowId:    "test-workflow-1",
		FailurePolicy: scheduler.FailurePolicy_FAIL_FAST,
		Tasks: []*scheduler.TaskSpec{
			{
				TaskId:     "task1",
				Command:    "echo hello",
				MaxRetries: 3,
			},
			{
				TaskId:       "task2",
				Dependencies: []string{"task1"},
				Command:      "echo world",
				MaxRetries:   3,
			},
		},
	}

	// Submit the workflow
	if err := coord.SubmitWorkflow(workflow); err != nil {
		t.Fatalf("failed to submit workflow: %v", err)
	}

	// Verify first task is ready
	if coord.GetReadyQueueSize() != 1 {
		t.Errorf("expected 1 ready task, got %d", coord.GetReadyQueueSize())
	}

	// Get task 1
	task1 := coord.GetReadyTask("worker-1")
	if task1 == nil {
		t.Fatal("expected task1 to be available")
	}

	if task1.TaskId != "task1" {
		t.Errorf("expected task1, got %s", task1.TaskId)
	}

	// Complete task 1
	if err := coord.CompleteTask("task1", task1.ExecutionId, "hello\n"); err != nil {
		t.Fatalf("failed to complete task1: %v", err)
	}

	// Verify task 2 is now ready
	if coord.GetReadyQueueSize() != 1 {
		t.Errorf("expected 1 ready task after task1 completion, got %d", coord.GetReadyQueueSize())
	}

	// Get task 2
	task2 := coord.GetReadyTask("worker-1")
	if task2 == nil {
		t.Fatal("expected task2 to be available")
	}

	if task2.TaskId != "task2" {
		t.Errorf("expected task2, got %s", task2.TaskId)
	}

	// Complete task 2
	if err := coord.CompleteTask("task2", task2.ExecutionId, "world\n"); err != nil {
		t.Fatalf("failed to complete task2: %v", err)
	}

	// Verify no more tasks
	if coord.GetReadyQueueSize() != 0 {
		t.Errorf("expected 0 ready tasks after completion, got %d", coord.GetReadyQueueSize())
	}

	coord.Stop()
}

// TestDAGValidation tests DAG validation
func TestDAGValidation(t *testing.T) {
	tests := []struct {
		name         string
		workflow     *scheduler.WorkflowSpec
		expectError  bool
		errorPattern string
	}{
		{
			name: "valid DAG",
			workflow: &scheduler.WorkflowSpec{
				WorkflowId: "valid-dag",
				Tasks: []*scheduler.TaskSpec{
					{TaskId: "a"},
					{TaskId: "b", Dependencies: []string{"a"}},
				},
			},
			expectError: false,
		},
		{
			name: "cycle detection",
			workflow: &scheduler.WorkflowSpec{
				WorkflowId: "cyclic-dag",
				Tasks: []*scheduler.TaskSpec{
					{TaskId: "a", Dependencies: []string{"b"}},
					{TaskId: "b", Dependencies: []string{"a"}},
				},
			},
			expectError:  true,
			errorPattern: "cycle",
		},
		{
			name: "missing dependency",
			workflow: &scheduler.WorkflowSpec{
				WorkflowId: "missing-dep",
				Tasks: []*scheduler.TaskSpec{
					{TaskId: "a", Dependencies: []string{"nonexistent"}},
				},
			},
			expectError:  true,
			errorPattern: "non-existent",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dag, err := coordinator.ValidateAndSort(tt.workflow)

			if tt.expectError && err == nil {
				t.Errorf("expected error, got nil")
			}

			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if !tt.expectError && dag == nil {
				t.Errorf("expected DAG to be returned")
			}
		})
	}
}

// TestIdempotency tests idempotent task completion
func TestIdempotency(t *testing.T) {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	stateStore, err := store.New("/tmp/test_idempotency.db")
	if err != nil {
		t.Fatalf("failed to create state store: %v", err)
	}
	defer stateStore.Close()

	coord := coordinator.NewCoordinator(stateStore, logger)
	if err := coord.Start(); err != nil {
		t.Fatalf("failed to start coordinator: %v", err)
	}

	workflow := &scheduler.WorkflowSpec{
		WorkflowId:    "idempotency-test",
		FailurePolicy: scheduler.FailurePolicy_FAIL_FAST,
		Tasks: []*scheduler.TaskSpec{
			{TaskId: "task1", Command: "echo test", MaxRetries: 3},
		},
	}

	if err := coord.SubmitWorkflow(workflow); err != nil {
		t.Fatalf("failed to submit workflow: %v", err)
	}

	task := coord.GetReadyTask("worker-1")
	if task == nil {
		t.Fatal("expected task")
	}

	execID := task.ExecutionId

	// Complete the task
	if err := coord.CompleteTask("task1", execID, "test\n"); err != nil {
		t.Fatalf("failed to complete task: %v", err)
	}

	// Try to complete with the same execution ID - should be idempotent
	if err := coord.CompleteTask("task1", execID, "test\n"); err != nil {
		t.Errorf("idempotent completion failed: %v", err)
	}

	// Try to complete with different execution ID - should fail
	wrongExecID := "wrong-execution-id"
	err = coord.CompleteTask("task1", wrongExecID, "test\n")
	if err == nil {
		t.Error("expected error when completing with wrong execution ID")
	}

	coord.Stop()
}

// TestHeartbeatDetection tests dead worker detection via heartbeat timeout
func TestHeartbeatDetection(t *testing.T) {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	stateStore, err := store.New("/tmp/test_heartbeat.db")
	if err != nil {
		t.Fatalf("failed to create state store: %v", err)
	}
	defer stateStore.Close()

	coord := coordinator.NewCoordinator(stateStore, logger)
	if err := coord.Start(); err != nil {
		t.Fatalf("failed to start coordinator: %v", err)
	}

	// Disable automatic heartbeat scanning by using a very long threshold
	// In production, heartbeat would be 5s scan interval, 15s dead threshold

	workflow := &scheduler.WorkflowSpec{
		WorkflowId:    "heartbeat-test",
		FailurePolicy: scheduler.FailurePolicy_FAIL_FAST,
		Tasks: []*scheduler.TaskSpec{
			{TaskId: "task1", Command: "echo test", MaxRetries: 3},
			{TaskId: "task2", Dependencies: []string{"task1"}, Command: "echo done", MaxRetries: 3},
		},
	}

	if err := coord.SubmitWorkflow(workflow); err != nil {
		t.Fatalf("failed to submit workflow: %v", err)
	}

	task1 := coord.GetReadyTask("worker-1")
	if task1 == nil {
		t.Fatal("expected task1")
	}

	// Simulate worker death by handling the dead worker
	coord.HandleDeadWorker("worker-1")

	// task1 should be re-enqueued
	if coord.GetReadyQueueSize() != 1 {
		t.Errorf("expected 1 ready task after worker death, got %d", coord.GetReadyQueueSize())
	}

	coord.Stop()
}

// TestCrashRecovery tests recovery from coordinator crash
func TestCrashRecovery(t *testing.T) {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	dbPath := "/tmp/test_recovery.db"

	// First coordinator instance
	stateStore1, err := store.New(dbPath)
	if err != nil {
		t.Fatalf("failed to create state store: %v", err)
	}

	coord1 := coordinator.NewCoordinator(stateStore1, logger)
	if err := coord1.Start(); err != nil {
		t.Fatalf("failed to start coordinator: %v", err)
	}

	workflow := &scheduler.WorkflowSpec{
		WorkflowId:    "recovery-test",
		FailurePolicy: scheduler.FailurePolicy_FAIL_FAST,
		Tasks: []*scheduler.TaskSpec{
			{TaskId: "task1", Command: "echo test", MaxRetries: 3},
			{TaskId: "task2", Dependencies: []string{"task1"}, Command: "echo done", MaxRetries: 3},
		},
	}

	if err := coord1.SubmitWorkflow(workflow); err != nil {
		t.Fatalf("failed to submit workflow: %v", err)
	}

	task1 := coord1.GetReadyTask("worker-1")
	if task1 == nil {
		t.Fatal("expected task1")
	}

	// Simulate coordinator crash by stopping without completing the task
	coord1.Stop()
	stateStore1.Close()

	// Second coordinator instance starts up and performs recovery
	stateStore2, err := store.New(dbPath)
	if err != nil {
		t.Fatalf("failed to create recovered state store: %v", err)
	}

	coord2 := coordinator.NewCoordinator(stateStore2, logger)

	// Call PrepareRecovery synchronously — it queries BoltDB and requeues tasks, no async needed
	if err := coord2.PrepareRecovery(); err != nil {
		t.Fatalf("PrepareRecovery failed: %v", err)
	}

	// Verify task is re-queued after recovery — PrepareRecovery transitions RUNNING -> QUEUED
	task, err := stateStore2.GetTask("task1")
	if err != nil {
		t.Fatalf("failed to get task after recovery: %v", err)
	}

	if task.State != scheduler.TaskState_TASK_QUEUED {
		t.Errorf("expected task to be QUEUED after recovery, got %v", task.State)
	}

	coord2.Stop()
	stateStore2.Close()
}
