package coordinator

import (
	"errors"
	"fmt"

	"github.com/aakashkolli/dag-scheduler/internal/proto/scheduler"
)

var (
	ErrCyclicDAG         = errors.New("DAG contains a cycle")
	ErrMissingDependency = errors.New("dependency references non-existent task")
	ErrDuplicateTaskID   = errors.New("duplicate task ID in workflow")
)

// DAG represents a directed acyclic graph of tasks
type DAG struct {
	tasks        map[string]*scheduler.TaskSpec
	dependencies map[string][]string // taskID -> list of dependencies
	downstream   map[string][]string // taskID -> list of tasks that depend on it
	inDegree     map[string]int32    // number of dependencies for each task
}

// ValidateAndSort validates the DAG for cycles and returns topologically sorted task IDs
// Also computes in-degree for each task
func ValidateAndSort(workflow *scheduler.WorkflowSpec) (*DAG, error) {
	dag := &DAG{
		tasks:        make(map[string]*scheduler.TaskSpec),
		dependencies: make(map[string][]string),
		downstream:   make(map[string][]string),
		inDegree:     make(map[string]int32),
	}

	// Check for duplicate task IDs and build task map
	for _, task := range workflow.Tasks {
		if _, exists := dag.tasks[task.TaskId]; exists {
			return nil, ErrDuplicateTaskID
		}
		dag.tasks[task.TaskId] = task
		dag.inDegree[task.TaskId] = 0
		dag.dependencies[task.TaskId] = task.Dependencies
		dag.downstream[task.TaskId] = []string{}
	}

	// Validate all dependencies exist
	for taskID, deps := range dag.dependencies {
		for _, dep := range deps {
			if _, exists := dag.tasks[dep]; !exists {
				return nil, fmt.Errorf("%w: task %s depends on non-existent task %s", ErrMissingDependency, taskID, dep)
			}
		}
	}

	// Build reverse dependency map and compute in-degrees
	for taskID, deps := range dag.dependencies {
		for _, dep := range deps {
			dag.downstream[dep] = append(dag.downstream[dep], taskID)
			dag.inDegree[taskID]++
		}
	}

	// Check for cycles using DFS
	if err := dag.hasCycle(); err != nil {
		return nil, err
	}

	return dag, nil
}

// hasCycle performs DFS to detect cycles in the DAG
func (d *DAG) hasCycle() error {
	// 0 = white (unvisited), 1 = gray (visiting), 2 = black (visited)
	color := make(map[string]int)
	for taskID := range d.tasks {
		color[taskID] = 0
	}

	var visit func(string) error
	visit = func(taskID string) error {
		if color[taskID] == 1 {
			// Back edge found - cycle exists
			return fmt.Errorf("%w: found cycle involving task %s", ErrCyclicDAG, taskID)
		}

		if color[taskID] == 2 {
			// Already fully visited
			return nil
		}

		color[taskID] = 1
		for _, dep := range d.dependencies[taskID] {
			if err := visit(dep); err != nil {
				return err
			}
		}
		color[taskID] = 2

		return nil
	}

	for taskID := range d.tasks {
		if color[taskID] == 0 {
			if err := visit(taskID); err != nil {
				return err
			}
		}
	}

	return nil
}

// GetInDegree returns the in-degree map for all tasks
func (d *DAG) GetInDegree() map[string]int32 {
	return d.inDegree
}

// GetDownstream returns tasks that depend on the given task
func (d *DAG) GetDownstream(taskID string) []string {
	return d.downstream[taskID]
}

// GetDependencies returns the dependencies of a task
func (d *DAG) GetDependencies(taskID string) []string {
	return d.dependencies[taskID]
}

// GetReadyTasks returns all tasks with in-degree 0 (no dependencies)
func (d *DAG) GetReadyTasks() []string {
	var ready []string
	for taskID, degree := range d.inDegree {
		if degree == 0 {
			ready = append(ready, taskID)
		}
	}
	return ready
}

// GetTaskSpec returns the spec for a task
func (d *DAG) GetTaskSpec(taskID string) *scheduler.TaskSpec {
	return d.tasks[taskID]
}

// GetAllTaskSpecs returns the full task spec map (taskID -> spec).
func (d *DAG) GetAllTaskSpecs() map[string]*scheduler.TaskSpec {
	return d.tasks
}

// GetAllDownstream returns the downstream adjacency map (taskID -> downstream task IDs).
func (d *DAG) GetAllDownstream() map[string][]string {
	return d.downstream
}
