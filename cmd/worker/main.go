package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/aakashkolli/dag-scheduler/internal/proto/scheduler"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const maxOutputBytes = 64 * 1024

func main() {
	var (
		coordinatorAddr = flag.String("coordinator", "localhost:50051", "Coordinator address")
		capacity        = flag.Int("capacity", 4, "Task execution capacity")
	)
	flag.Parse()

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	conn, err := grpc.Dial(*coordinatorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Fatal("failed to connect to coordinator", zap.Error(err))
	}
	defer conn.Close()

	client := scheduler.NewSchedulerClient(conn)
	workerID := fmt.Sprintf("worker-%s", uuid.New().String()[:8])

	logger.Info("worker starting",
		zap.String("worker_id", workerID),
		zap.String("coordinator", *coordinatorAddr),
		zap.Int("capacity", *capacity),
	)

	w := NewWorker(workerID, client, int32(*capacity), logger)

	go w.Run()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Info("worker shutting down", zap.String("worker_id", workerID))
	w.Stop()
	logger.Info("worker stopped", zap.String("worker_id", workerID))
}

// Worker represents a task execution worker
type Worker struct {
	id        string
	client    scheduler.SchedulerClient
	capacity  int32
	logger    *zap.Logger
	ctx       context.Context
	cancel    context.CancelFunc
	runningMu sync.Mutex
	running   map[string]bool
}

// NewWorker creates a new worker
func NewWorker(id string, client scheduler.SchedulerClient, capacity int32, logger *zap.Logger) *Worker {
	ctx, cancel := context.WithCancel(context.Background())
	return &Worker{
		id:       id,
		client:   client,
		capacity: capacity,
		logger:   logger,
		ctx:      ctx,
		cancel:   cancel,
		running:  make(map[string]bool),
	}
}

// Stop signals the worker to shut down.
func (w *Worker) Stop() { w.cancel() }

func (w *Worker) runningCount() int {
	w.runningMu.Lock()
	defer w.runningMu.Unlock()
	return len(w.running)
}

// Run is the main loop — reconnects with exponential backoff on stream failure.
func (w *Worker) Run() {
	backoff := time.Second
	const maxBackoff = 30 * time.Second

	for {
		if w.ctx.Err() != nil {
			return
		}

		if err := w.runOnce(); err != nil && w.ctx.Err() == nil {
			w.logger.Warn("stream disconnected, reconnecting",
				zap.Duration("backoff", backoff),
				zap.Error(err),
			)
			select {
			case <-time.After(backoff):
			case <-w.ctx.Done():
				return
			}
			if backoff < maxBackoff {
				backoff *= 2
			}
			continue
		}
		return
	}
}

// runOnce opens a single stream and processes messages until the stream dies or ctx is cancelled.
func (w *Worker) runOnce() error {
	stream, err := w.client.WorkerStream(w.ctx)
	if err != nil {
		return err
	}
	defer stream.CloseSend()

	recvErrCh := make(chan error, 1)

	go func() {
		recvErrCh <- w.recvLoop(stream)
	}()

	ticker := time.NewTicker(100 * time.Millisecond)
	hbTicker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	defer hbTicker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return nil

		case err := <-recvErrCh:
			return err

		case <-hbTicker.C:
			w.runningMu.Lock()
			ids := make([]string, 0, len(w.running))
			for id := range w.running {
				ids = append(ids, id)
			}
			w.runningMu.Unlock()

			msg := &scheduler.WorkerMessage{
				WorkerId: w.id,
				Payload: &scheduler.WorkerMessage_Heartbeat{
					Heartbeat: &scheduler.Heartbeat{
						TimestampUnixMs: time.Now().UnixMilli(),
						RunningTaskIds:  ids,
					},
				},
			}
			if err := stream.Send(msg); err != nil && err != io.EOF {
				return err
			}

		case <-ticker.C:
			available := w.capacity - int32(w.runningCount())
			if available <= 0 {
				continue
			}
			msg := &scheduler.WorkerMessage{
				WorkerId: w.id,
				Payload: &scheduler.WorkerMessage_TaskRequest{
					TaskRequest: &scheduler.TaskRequest{Capacity: available},
				},
			}
			if err := stream.Send(msg); err != nil && err != io.EOF {
				return err
			}
		}
	}
}

// recvLoop reads coordinator messages and dispatches them.
func (w *Worker) recvLoop(stream scheduler.Scheduler_WorkerStreamClient) error {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch payload := msg.Payload.(type) {
		case *scheduler.CoordinatorMessage_TaskAssignment:
			go w.executeTask(stream, payload.TaskAssignment)

		case *scheduler.CoordinatorMessage_NoTask:
			// Coordinator told us to wait; the request ticker will retry.
			time.Sleep(time.Duration(payload.NoTask.WaitMs) * time.Millisecond)

		case *scheduler.CoordinatorMessage_Ping:
			w.logger.Debug("ping from coordinator")
		}
	}
}

// executeTask runs the task command and reports the result back on the stream.
func (w *Worker) executeTask(stream scheduler.Scheduler_WorkerStreamClient, assignment *scheduler.TaskAssignment) {
	w.runningMu.Lock()
	w.running[assignment.TaskId] = true
	w.runningMu.Unlock()
	defer func() {
		w.runningMu.Lock()
		delete(w.running, assignment.TaskId)
		w.runningMu.Unlock()
	}()

	// Build a per-task context that respects both worker shutdown and task timeout.
	taskCtx := w.ctx
	if assignment.TimeoutSeconds > 0 {
		var cancel context.CancelFunc
		taskCtx, cancel = context.WithTimeout(w.ctx, time.Duration(assignment.TimeoutSeconds)*time.Second)
		defer cancel()
	}

	start := time.Now()
	cmd := exec.CommandContext(taskCtx, "sh", "-c", assignment.Command)

	env := os.Environ()
	for k, v := range assignment.Env {
		env = append(env, fmt.Sprintf("%s=%s", k, v))
	}
	cmd.Env = env

	output, execErr := cmd.CombinedOutput()
	duration := time.Since(start)

	// Truncate output to 64KB per spec.
	outputStr := string(output)
	if len(outputStr) > maxOutputBytes {
		outputStr = outputStr[:maxOutputBytes] + "\n[output truncated]"
	}

	result := &scheduler.TaskResult{
		TaskId:      assignment.TaskId,
		ExecutionId: assignment.ExecutionId,
		DurationMs:  duration.Milliseconds(),
		Output:      outputStr,
	}

	if execErr == nil {
		result.Success = true
		w.logger.Info("task completed",
			zap.String("task_id", assignment.TaskId),
			zap.Int64("duration_ms", result.DurationMs),
		)
	} else {
		errMsg := fmt.Sprintf("execution error: %v", execErr)
		if taskCtx.Err() == context.DeadlineExceeded {
			errMsg = fmt.Sprintf("task timed out after %ds", assignment.TimeoutSeconds)
		}
		result.Error = errMsg
		w.logger.Warn("task failed",
			zap.String("task_id", assignment.TaskId),
			zap.String("error", errMsg),
		)
	}

	msg := &scheduler.WorkerMessage{
		WorkerId: w.id,
		Payload:  &scheduler.WorkerMessage_TaskResult{TaskResult: result},
	}
	if err := stream.Send(msg); err != nil {
		w.logger.Error("failed to send task result",
			zap.String("task_id", assignment.TaskId),
			zap.Error(err),
		)
	}
}
