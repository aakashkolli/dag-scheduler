package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aakashkolli/dag-scheduler/internal/coordinator"
	"github.com/aakashkolli/dag-scheduler/internal/proto/scheduler"
	"github.com/aakashkolli/dag-scheduler/internal/store"
	"github.com/aakashkolli/dag-scheduler/internal/worker"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func main() {
	var (
		port          = flag.Int("port", 50051, "gRPC server port")
		metricsPort   = flag.Int("metrics-port", 9090, "Prometheus metrics and pprof HTTP port")
		dbPath        = flag.String("db", "/tmp/scheduler.db", "BoltDB database path")
		scanInterval  = flag.Duration("scan-interval", 5*time.Second, "heartbeat scan interval")
		deadThreshold = flag.Duration("dead-threshold", 15*time.Second, "duration before declaring a worker dead")
	)
	flag.Parse()

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	stateStore, err := store.New(*dbPath)
	if err != nil {
		logger.Fatal("failed to initialize state store", zap.Error(err))
	}
	defer stateStore.Close()

	coord := coordinator.NewCoordinator(
		stateStore,
		logger,
		coordinator.WithHeartbeatConfig(*scanInterval, *deadThreshold),
	)

	// Always recover on startup: hydrate in-memory caches and re-enqueue
	// any tasks that were RUNNING when the coordinator last crashed.
	if err := coord.LoadStateFromDB(); err != nil {
		logger.Error("failed to load state from DB", zap.Error(err))
	}
	if err := coord.PrepareRecovery(); err != nil {
		logger.Error("crash recovery prepare failed", zap.Error(err))
	}

	if err := coord.Start(); err != nil {
		logger.Fatal("failed to start coordinator", zap.Error(err))
	}

	// Metrics + pprof + dashboard on a separate port so it never blocks gRPC traffic.
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		dash := newDashboardServer(stateStore)
		dash.register(mux)
		addr := fmt.Sprintf(":%d", *metricsPort)
		logger.Info("metrics+dashboard server starting", zap.String("addr", addr))
		if err := http.ListenAndServe(addr, mux); err != nil {
			logger.Error("metrics server error", zap.Error(err))
		}
	}()

	grpcServer := grpc.NewServer()
	svc := &SchedulerService{
		coordinator: coord,
		store:       stateStore,
		logger:      logger,
	}
	scheduler.RegisterSchedulerServer(grpcServer, svc)

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		logger.Fatal("failed to listen", zap.Error(err))
	}

	logger.Info("coordinator starting",
		zap.Int("port", *port),
		zap.String("db", *dbPath),
		zap.Duration("scan_interval", *scanInterval),
		zap.Duration("dead_threshold", *deadThreshold),
	)

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			logger.Fatal("gRPC server error", zap.Error(err))
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Info("shutting down coordinator")
	coord.Stop()
	grpcServer.GracefulStop()
	listener.Close()
	logger.Info("coordinator stopped")
}

// SchedulerService implements the gRPC Scheduler service.
type SchedulerService struct {
	coordinator *coordinator.Coordinator
	store       *store.StateStore
	logger      *zap.Logger
	scheduler.UnimplementedSchedulerServer
}

// SubmitWorkflow submits a new workflow.
func (s *SchedulerService) SubmitWorkflow(ctx context.Context, workflow *scheduler.WorkflowSpec) (*scheduler.SubmitWorkflowResponse, error) {
	if err := s.coordinator.SubmitWorkflow(workflow); err != nil {
		return &scheduler.SubmitWorkflowResponse{
			WorkflowId: workflow.WorkflowId,
			Success:    false,
			Error:      err.Error(),
		}, nil
	}
	return &scheduler.SubmitWorkflowResponse{
		WorkflowId: workflow.WorkflowId,
		Success:    true,
	}, nil
}

// GetWorkflowStatus returns the current state and per-task statuses for a workflow.
func (s *SchedulerService) GetWorkflowStatus(ctx context.Context, req *scheduler.GetWorkflowStatusRequest) (*scheduler.WorkflowStatus, error) {
	wfState, err := s.store.GetWorkflowState(req.WorkflowId)
	if err != nil {
		return nil, err
	}

	taskStatuses, err := s.store.GetTaskStatusesForWorkflow(req.WorkflowId)
	if err != nil {
		return nil, err
	}

	return &scheduler.WorkflowStatus{
		WorkflowId:        req.WorkflowId,
		State:             wfState.State,
		TaskStatuses:      taskStatuses,
		SubmittedAtUnixMs: wfState.SubmittedAtMs,
		CompletedAtUnixMs: wfState.CompletedAtMs,
	}, nil
}

// CancelWorkflow cancels a running workflow and all non-terminal tasks.
func (s *SchedulerService) CancelWorkflow(ctx context.Context, req *scheduler.CancelWorkflowRequest) (*scheduler.CancelWorkflowResponse, error) {
	if err := s.coordinator.CancelWorkflow(req.WorkflowId); err != nil {
		return &scheduler.CancelWorkflowResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}
	return &scheduler.CancelWorkflowResponse{Success: true}, nil
}

// WorkerStream handles a worker's bidirectional stream.
func (s *SchedulerService) WorkerStream(stream scheduler.Scheduler_WorkerStreamServer) error {
	h := worker.NewStreamHandler(s.coordinator, s.logger)
	return h.HandleStream(stream)
}
