package worker

import (
	"io"

	"github.com/aakashkolli/dag-scheduler/internal/proto/scheduler"
	coord "github.com/aakashkolli/dag-scheduler/internal/coordinator"
	"go.uber.org/zap"
)

// StreamHandler handles a worker's bidirectional gRPC stream
type StreamHandler struct {
	coordinator *coord.Coordinator
	logger      *zap.Logger
}

// NewStreamHandler creates a new stream handler
func NewStreamHandler(coordinator *coord.Coordinator, logger *zap.Logger) *StreamHandler {
	return &StreamHandler{
		coordinator: coordinator,
		logger:      logger,
	}
}

// HandleStream handles a worker's bidirectional stream
func (h *StreamHandler) HandleStream(stream scheduler.Scheduler_WorkerStreamServer) error {
	ctx := stream.Context()
	workerID := ""

	for {
		select {
		case <-ctx.Done():
			if h.logger != nil {
				h.logger.Info("worker stream context cancelled",
					zap.String("worker_id", workerID),
				)
			}
			return ctx.Err()
		default:
		}

		// Receive message from worker
		msg, err := stream.Recv()
		if err == io.EOF {
			if h.logger != nil {
				h.logger.Info("worker stream closed",
					zap.String("worker_id", workerID),
				)
			}
			return nil
		}
		if err != nil {
			if h.logger != nil {
				h.logger.Error("error receiving worker message",
					zap.Error(err),
				)
			}
			return err
		}

		// First message must identify the worker
		if workerID == "" {
			workerID = msg.WorkerId
			if h.logger != nil {
				h.logger.Info("worker connected",
					zap.String("worker_id", workerID),
				)
			}
		}

		// Handle different message types
		switch payload := msg.Payload.(type) {
		case *scheduler.WorkerMessage_TaskRequest:
			if err := h.handleTaskRequest(stream, workerID, payload.TaskRequest); err != nil {
				return err
			}

		case *scheduler.WorkerMessage_Heartbeat:
			h.handleHeartbeat(workerID, payload.Heartbeat)

		case *scheduler.WorkerMessage_TaskResult:
			if err := h.handleTaskResult(workerID, payload.TaskResult); err != nil {
				if h.logger != nil {
					h.logger.Error("error handling task result",
						zap.String("worker_id", workerID),
						zap.Error(err),
					)
				}
				// Don't fail the stream for this error
			}

		default:
			if h.logger != nil {
				h.logger.Warn("unknown message type",
					zap.String("worker_id", workerID),
				)
			}
		}
	}
}

// handleTaskRequest processes a worker's task request
func (h *StreamHandler) handleTaskRequest(stream scheduler.Scheduler_WorkerStreamServer, workerID string, _ *scheduler.TaskRequest) error {
	task := h.coordinator.GetReadyTask(workerID)

	if task != nil {
		return stream.Send(&scheduler.CoordinatorMessage{
			Payload: &scheduler.CoordinatorMessage_TaskAssignment{
				TaskAssignment: task,
			},
		})
	}

	return stream.Send(&scheduler.CoordinatorMessage{
		Payload: &scheduler.CoordinatorMessage_NoTask{
			NoTask: &scheduler.NoTask{
				WaitMs: 1000,
			},
		},
	})
}

// handleHeartbeat processes a worker's heartbeat
func (h *StreamHandler) handleHeartbeat(workerID string, heartbeat *scheduler.Heartbeat) {
	h.coordinator.UpdateWorkerHeartbeat(workerID, heartbeat.TimestampUnixMs, heartbeat.RunningTaskIds)

	if h.logger != nil {
		h.logger.Debug("worker heartbeat",
			zap.String("worker_id", workerID),
			zap.Int("running_tasks", len(heartbeat.RunningTaskIds)),
		)
	}
}

// handleTaskResult processes a task completion/failure result
func (h *StreamHandler) handleTaskResult(workerID string, result *scheduler.TaskResult) error {
	if result.Success {
		// Task completed successfully
		if err := h.coordinator.CompleteTask(result.TaskId, result.ExecutionId, result.Output); err != nil {
			if h.logger != nil {
				h.logger.Warn("failed to complete task",
					zap.String("task_id", result.TaskId),
					zap.String("worker_id", workerID),
					zap.Error(err),
				)
			}
			// Idempotency: if task already completed, this is not an error
			return nil
		}

		if h.logger != nil {
			h.logger.Info("task completed",
				zap.String("task_id", result.TaskId),
				zap.String("worker_id", workerID),
				zap.Int64("duration_ms", result.DurationMs),
			)
		}
	} else {
		// Task failed
		if err := h.coordinator.FailTask(result.TaskId, result.ExecutionId, result.Error); err != nil {
			if h.logger != nil {
				h.logger.Warn("failed to fail task",
					zap.String("task_id", result.TaskId),
					zap.String("worker_id", workerID),
					zap.Error(err),
				)
			}
			return nil
		}

		if h.logger != nil {
			h.logger.Info("task failed",
				zap.String("task_id", result.TaskId),
				zap.String("worker_id", workerID),
				zap.String("error", result.Error),
			)
		}
	}

	return nil
}
