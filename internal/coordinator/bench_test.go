package coordinator

import (
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/aakashkolli/dag-scheduler/internal/proto/scheduler"
	"github.com/aakashkolli/dag-scheduler/internal/store"
	"go.uber.org/zap"
)

func newBenchCoordinator(b *testing.B) (*Coordinator, func()) {
	b.Helper()
	f, err := os.CreateTemp("", "bench-coord-*.db")
	if err != nil {
		b.Fatalf("CreateTemp: %v", err)
	}
	f.Close()
	st, err := store.New(f.Name())
	if err != nil {
		b.Fatalf("store.New: %v", err)
	}
	coord := NewCoordinator(st, zap.NewNop())
	coord.Start()
	return coord, func() {
		coord.Stop()
		st.Close()
		os.Remove(f.Name())
	}
}

// BenchmarkWorkflowSubmission measures workflow submission throughput.
// PRD target: > 100 workflows/sec with 10-task linear DAGs.
func BenchmarkWorkflowSubmission(b *testing.B) {
	coord, cleanup := newBenchCoordinator(b)
	defer cleanup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tasks := make([]*scheduler.TaskSpec, 10)
		for j := 0; j < 10; j++ {
			tasks[j] = &scheduler.TaskSpec{
				TaskId:  fmt.Sprintf("wf%d-t%d", i, j),
				Command: "echo x",
			}
			if j > 0 {
				tasks[j].Dependencies = []string{fmt.Sprintf("wf%d-t%d", i, j-1)}
			}
		}
		wf := &scheduler.WorkflowSpec{
			WorkflowId: fmt.Sprintf("bench-wf-%d", i),
			Tasks:      tasks,
		}
		if err := coord.SubmitWorkflow(wf); err != nil {
			b.Fatalf("SubmitWorkflow: %v", err)
		}
	}
}

// BenchmarkTaskAssignmentLatency measures the time to dequeue and assign a task.
// PRD target: < 50ms from ready to worker receipt (this measures the coordinator side).
func BenchmarkTaskAssignmentLatency(b *testing.B) {
	coord, cleanup := newBenchCoordinator(b)
	defer cleanup()

	// Pre-fill queue with isolated single-task workflows.
	for i := 0; i < b.N; i++ {
		wf := &scheduler.WorkflowSpec{
			WorkflowId: fmt.Sprintf("lat-wf-%d", i),
			Tasks: []*scheduler.TaskSpec{
				{TaskId: fmt.Sprintf("lat-t-%d", i), Command: "echo x"},
			},
		}
		if err := coord.SubmitWorkflow(wf); err != nil {
			b.Fatalf("SubmitWorkflow: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		task := coord.GetReadyTask("bench-worker")
		if task == nil {
			b.Fatalf("no task available at iteration %d", i)
		}
	}
}

// BenchmarkConcurrentTaskCompletion measures throughput of parallel CompleteTask calls.
// Simulates 10 concurrent workers each completing tasks.
func BenchmarkConcurrentTaskCompletion(b *testing.B) {
	coord, cleanup := newBenchCoordinator(b)
	defer cleanup()

	const numTasks = 5000
	assignments := make([]*scheduler.TaskAssignment, 0, numTasks)
	var mu sync.Mutex

	for i := 0; i < numTasks; i++ {
		wf := &scheduler.WorkflowSpec{
			WorkflowId: fmt.Sprintf("conc-wf-%d", i),
			Tasks: []*scheduler.TaskSpec{
				{TaskId: fmt.Sprintf("conc-t-%d", i), Command: "echo x"},
			},
		}
		coord.SubmitWorkflow(wf)
	}
	for i := 0; i < numTasks; i++ {
		if a := coord.GetReadyTask("setup-worker"); a != nil {
			assignments = append(assignments, a)
		}
	}

	idx := 0
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.Lock()
			if idx >= len(assignments) {
				mu.Unlock()
				return
			}
			a := assignments[idx]
			idx++
			mu.Unlock()
			coord.CompleteTask(a.TaskId, a.ExecutionId, "ok")
		}
	})
}

// BenchmarkDAGValidation measures the cost of cycle detection + in-degree computation.
func BenchmarkDAGValidation(b *testing.B) {
	// Build a 100-task fan-out DAG: root → 99 children
	wf := &scheduler.WorkflowSpec{WorkflowId: "bench-dag"}
	wf.Tasks = append(wf.Tasks, &scheduler.TaskSpec{TaskId: "root"})
	for i := 1; i <= 99; i++ {
		wf.Tasks = append(wf.Tasks, &scheduler.TaskSpec{
			TaskId:       fmt.Sprintf("child-%d", i),
			Dependencies: []string{"root"},
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ValidateAndSort(wf); err != nil {
			b.Fatalf("ValidateAndSort: %v", err)
		}
	}
}
