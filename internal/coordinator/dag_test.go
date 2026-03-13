package coordinator

import (
	"testing"

	"github.com/aakashkolli/dag-scheduler/internal/proto/scheduler"
)

func TestValidateAndSort_ValidLinear(t *testing.T) {
	wf := &scheduler.WorkflowSpec{
		WorkflowId: "linear",
		Tasks: []*scheduler.TaskSpec{
			{TaskId: "a"},
			{TaskId: "b", Dependencies: []string{"a"}},
			{TaskId: "c", Dependencies: []string{"b"}},
		},
	}
	dag, err := ValidateAndSort(wf)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	ready := dag.GetReadyTasks()
	if len(ready) != 1 || ready[0] != "a" {
		t.Errorf("expected [a] as ready tasks, got %v", ready)
	}
	inDeg := dag.GetInDegree()
	if inDeg["b"] != 1 {
		t.Errorf("expected b in_degree=1, got %d", inDeg["b"])
	}
	if inDeg["c"] != 1 {
		t.Errorf("expected c in_degree=1, got %d", inDeg["c"])
	}
}

func TestValidateAndSort_Diamond(t *testing.T) {
	wf := &scheduler.WorkflowSpec{
		WorkflowId: "diamond",
		Tasks: []*scheduler.TaskSpec{
			{TaskId: "root"},
			{TaskId: "left", Dependencies: []string{"root"}},
			{TaskId: "right", Dependencies: []string{"root"}},
			{TaskId: "merge", Dependencies: []string{"left", "right"}},
		},
	}
	dag, err := ValidateAndSort(wf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	inDeg := dag.GetInDegree()
	if inDeg["merge"] != 2 {
		t.Errorf("expected merge in_degree=2, got %d", inDeg["merge"])
	}
	ready := dag.GetReadyTasks()
	if len(ready) != 1 || ready[0] != "root" {
		t.Errorf("expected [root] as ready, got %v", ready)
	}
}

func TestValidateAndSort_MultipleRoots(t *testing.T) {
	wf := &scheduler.WorkflowSpec{
		WorkflowId: "multi-root",
		Tasks: []*scheduler.TaskSpec{
			{TaskId: "a"},
			{TaskId: "b"},
			{TaskId: "c", Dependencies: []string{"a", "b"}},
		},
	}
	dag, err := ValidateAndSort(wf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ready := dag.GetReadyTasks()
	if len(ready) != 2 {
		t.Errorf("expected 2 ready tasks, got %d: %v", len(ready), ready)
	}
}

func TestValidateAndSort_Cycle(t *testing.T) {
	wf := &scheduler.WorkflowSpec{
		WorkflowId: "cyclic",
		Tasks: []*scheduler.TaskSpec{
			{TaskId: "a", Dependencies: []string{"c"}},
			{TaskId: "b", Dependencies: []string{"a"}},
			{TaskId: "c", Dependencies: []string{"b"}},
		},
	}
	_, err := ValidateAndSort(wf)
	if err == nil {
		t.Fatal("expected cycle error")
	}
}

func TestValidateAndSort_SelfLoop(t *testing.T) {
	wf := &scheduler.WorkflowSpec{
		WorkflowId: "self-loop",
		Tasks: []*scheduler.TaskSpec{
			{TaskId: "a", Dependencies: []string{"a"}},
		},
	}
	_, err := ValidateAndSort(wf)
	if err == nil {
		t.Fatal("expected cycle error for self-loop")
	}
}

func TestValidateAndSort_DuplicateTaskID(t *testing.T) {
	wf := &scheduler.WorkflowSpec{
		WorkflowId: "dup",
		Tasks: []*scheduler.TaskSpec{
			{TaskId: "a"},
			{TaskId: "a"},
		},
	}
	_, err := ValidateAndSort(wf)
	if err == nil {
		t.Fatal("expected duplicate task ID error")
	}
}

func TestValidateAndSort_MissingDependency(t *testing.T) {
	wf := &scheduler.WorkflowSpec{
		WorkflowId: "missing",
		Tasks: []*scheduler.TaskSpec{
			{TaskId: "a", Dependencies: []string{"nonexistent"}},
		},
	}
	_, err := ValidateAndSort(wf)
	if err == nil {
		t.Fatal("expected missing dependency error")
	}
}

func TestDAG_GetDownstream(t *testing.T) {
	wf := &scheduler.WorkflowSpec{
		WorkflowId: "downstream",
		Tasks: []*scheduler.TaskSpec{
			{TaskId: "root"},
			{TaskId: "child1", Dependencies: []string{"root"}},
			{TaskId: "child2", Dependencies: []string{"root"}},
		},
	}
	dag, err := ValidateAndSort(wf)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ds := dag.GetDownstream("root")
	if len(ds) != 2 {
		t.Errorf("expected 2 downstream of root, got %d: %v", len(ds), ds)
	}
	// Leaf has no downstream
	if len(dag.GetDownstream("child1")) != 0 {
		t.Errorf("expected empty downstream for leaf")
	}
}

func TestDAG_GetAllTaskSpecs(t *testing.T) {
	wf := &scheduler.WorkflowSpec{
		WorkflowId: "specs",
		Tasks: []*scheduler.TaskSpec{
			{TaskId: "a", Command: "echo a"},
			{TaskId: "b", Command: "echo b"},
		},
	}
	dag, _ := ValidateAndSort(wf)
	specs := dag.GetAllTaskSpecs()
	if len(specs) != 2 {
		t.Errorf("expected 2 specs, got %d", len(specs))
	}
	if specs["a"].Command != "echo a" {
		t.Errorf("unexpected command: %s", specs["a"].Command)
	}
}
