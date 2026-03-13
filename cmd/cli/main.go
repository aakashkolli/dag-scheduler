package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aakashkolli/dag-scheduler/internal/proto/scheduler"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "Coordinator address")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	client := scheduler.NewSchedulerClient(conn)

	switch args[0] {
	case "submit":
		runSubmit(client, args[1:])
	case "status":
		runStatus(client, args[1:])
	case "cancel":
		runCancel(client, args[1:])
	default:
		log.Fatalf("unknown command: %s", args[0])
	}
}

func printUsage() {
	fmt.Println("Usage: cli [--addr <host:port>] <command> [args]")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println("  submit <workflow.json>         Submit a workflow from a JSON file")
	fmt.Println("  status <workflow_id> [--watch] Show workflow status (--watch polls every 2s)")
	fmt.Println("  cancel <workflow_id>           Cancel a running workflow")
}

// ── submit ──────────────────────────────────────────────────────────────────

func runSubmit(client scheduler.SchedulerClient, args []string) {
	if len(args) < 1 {
		log.Fatal("Usage: submit <workflow.json>")
	}
	data, err := os.ReadFile(args[0])
	if err != nil {
		log.Fatalf("failed to read file: %v", err)
	}

	var payload struct {
		WorkflowID    string `json:"workflow_id"`
		FailurePolicy string `json:"failure_policy"`
		Tasks         []struct {
			TaskID       string            `json:"task_id"`
			Dependencies []string          `json:"dependencies"`
			Command      string            `json:"command"`
			Env          map[string]string `json:"env"`
			TimeoutSec   int32             `json:"timeout_seconds"`
			MaxRetries   int32             `json:"max_retries"`
		} `json:"tasks"`
	}

	if err := json.Unmarshal(data, &payload); err != nil {
		log.Fatalf("failed to parse JSON: %v", err)
	}

	policy := scheduler.FailurePolicy_FAIL_FAST
	switch strings.ToUpper(payload.FailurePolicy) {
	case "SKIP_DOWNSTREAM":
		policy = scheduler.FailurePolicy_SKIP_DOWNSTREAM
	case "CONTINUE_INDEPENDENT":
		policy = scheduler.FailurePolicy_CONTINUE_INDEPENDENT
	}

	spec := &scheduler.WorkflowSpec{
		WorkflowId:    payload.WorkflowID,
		FailurePolicy: policy,
	}
	for _, t := range payload.Tasks {
		spec.Tasks = append(spec.Tasks, &scheduler.TaskSpec{
			TaskId:         t.TaskID,
			Dependencies:   t.Dependencies,
			Command:        t.Command,
			Env:            t.Env,
			TimeoutSeconds: t.TimeoutSec,
			MaxRetries:     t.MaxRetries,
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, err := client.SubmitWorkflow(ctx, spec)
	if err != nil {
		log.Fatalf("SubmitWorkflow RPC failed: %v", err)
	}
	if !res.Success {
		log.Fatalf("submit error: %s", res.Error)
	}
	fmt.Printf("Submitted workflow: %s\n", res.WorkflowId)
	fmt.Printf("Run `cli status %s --watch` to follow execution.\n", res.WorkflowId)
}

// ── status ───────────────────────────────────────────────────────────────────

func runStatus(client scheduler.SchedulerClient, args []string) {
	if len(args) < 1 {
		log.Fatal("Usage: status <workflow_id> [--watch]")
	}
	workflowID := args[0]
	watch := len(args) >= 2 && args[1] == "--watch"

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		res, err := client.GetWorkflowStatus(ctx, &scheduler.GetWorkflowStatusRequest{WorkflowId: workflowID})
		cancel()
		if err != nil {
			log.Fatalf("GetWorkflowStatus failed: %v", err)
		}

		if watch {
			fmt.Print("\033[H\033[2J") // ANSI clear screen
		}
		printWorkflowStatus(res)

		if !watch || isTerminalState(res.State) {
			break
		}
		time.Sleep(2 * time.Second)
	}
}

func printWorkflowStatus(res *scheduler.WorkflowStatus) {
	stateStr := res.State.String()
	stateColor := colorForWorkflowState(res.State)

	fmt.Printf("╔══════════════════════════════════════════════════╗\n")
	fmt.Printf("  Workflow  : %s\n", res.WorkflowId)
	fmt.Printf("  State     : %s%s%s\n", stateColor, stateStr, colorReset)
	if res.SubmittedAtUnixMs > 0 {
		t := time.UnixMilli(res.SubmittedAtUnixMs)
		fmt.Printf("  Submitted : %s\n", t.Format("2006-01-02 15:04:05"))
	}
	if res.CompletedAtUnixMs > 0 {
		t := time.UnixMilli(res.CompletedAtUnixMs)
		dur := time.Duration(res.CompletedAtUnixMs-res.SubmittedAtUnixMs) * time.Millisecond
		fmt.Printf("  Completed : %s  (elapsed %s)\n", t.Format("2006-01-02 15:04:05"), dur.Round(time.Millisecond))
	}
	fmt.Printf("╚══════════════════════════════════════════════════╝\n")
	fmt.Printf("\n  Tasks (%d):\n", len(res.TaskStatuses))

	for _, t := range res.TaskStatuses {
		icon := stateIcon(t.State)
		color := colorForTaskState(t.State)
		attempts := ""
		if t.AttemptCount > 0 {
			attempts = fmt.Sprintf(" [%d attempt(s)]", t.AttemptCount)
		}
		fmt.Printf("    %s%s  %-32s %s%s%s\n",
			color, icon, t.TaskId,
			t.State.String(), attempts, colorReset,
		)
		if t.Error != "" {
			fmt.Printf("         error: %s\n", t.Error)
		}
	}
	fmt.Println()
}

func stateIcon(s scheduler.TaskState) string {
	switch s {
	case scheduler.TaskState_TASK_COMPLETE:
		return "✓"
	case scheduler.TaskState_TASK_FAILED:
		return "✗"
	case scheduler.TaskState_TASK_RUNNING:
		return "▶"
	case scheduler.TaskState_TASK_QUEUED:
		return "⋯"
	case scheduler.TaskState_TASK_SKIPPED:
		return "⊘"
	default:
		return "○"
	}
}

const (
	colorReset  = "\033[0m"
	colorGreen  = "\033[32m"
	colorRed    = "\033[31m"
	colorYellow = "\033[33m"
	colorCyan   = "\033[36m"
	colorGray   = "\033[90m"
)

func colorForTaskState(s scheduler.TaskState) string {
	switch s {
	case scheduler.TaskState_TASK_COMPLETE:
		return colorGreen
	case scheduler.TaskState_TASK_FAILED:
		return colorRed
	case scheduler.TaskState_TASK_RUNNING:
		return colorCyan
	case scheduler.TaskState_TASK_QUEUED:
		return colorYellow
	case scheduler.TaskState_TASK_SKIPPED:
		return colorGray
	default:
		return colorGray
	}
}

func colorForWorkflowState(s scheduler.WorkflowState) string {
	switch s {
	case scheduler.WorkflowState_WORKFLOW_COMPLETE:
		return colorGreen
	case scheduler.WorkflowState_WORKFLOW_FAILED:
		return colorRed
	case scheduler.WorkflowState_WORKFLOW_CANCELLED:
		return colorGray
	default:
		return colorCyan
	}
}

func isTerminalState(s scheduler.WorkflowState) bool {
	return s == scheduler.WorkflowState_WORKFLOW_COMPLETE ||
		s == scheduler.WorkflowState_WORKFLOW_FAILED ||
		s == scheduler.WorkflowState_WORKFLOW_CANCELLED
}

// ── cancel ───────────────────────────────────────────────────────────────────

func runCancel(client scheduler.SchedulerClient, args []string) {
	if len(args) < 1 {
		log.Fatal("Usage: cancel <workflow_id>")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := client.CancelWorkflow(ctx, &scheduler.CancelWorkflowRequest{WorkflowId: args[0]})
	if err != nil {
		log.Fatalf("CancelWorkflow RPC failed: %v", err)
	}
	if !res.Success {
		log.Fatalf("cancel error: %s", res.Error)
	}
	fmt.Printf("Cancelled workflow: %s\n", args[0])
}
