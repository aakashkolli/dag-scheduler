# dag-scheduler

A fault-tolerant distributed DAG task scheduler in Go. Clients submit directed
acyclic graph workflows over gRPC. The coordinator resolves dependencies,
dispatches tasks to a pull-based worker pool via bidirectional streams, and
recovers from both worker and coordinator crashes without losing state.

## Quick Start

**Start the coordinator**
```bash
go run ./cmd/coordinator --port 50051 --db /tmp/scheduler.db
```

**Start workers**
```bash
go run ./cmd/worker --coordinator localhost:50051 --capacity 4
```

**Submit a workflow**
```bash
go run ./cmd/cli submit workflow.json
go run ./cmd/cli status <workflow_id> --watch
```

**With Docker Compose**
```bash
docker compose up --build
```

## Architecture

The coordinator validates DAG structure, persists workflow state in BoltDB, and
maintains an in-memory FIFO ready queue. Workers pull tasks over bidirectional
gRPC streams. Dead workers are detected via explicit heartbeat timestamps and
their tasks are automatically requeued.

## Testing

```bash
go test ./... -race -count=1
go test ./tests/integration/... -v
go test ./internal/coordinator/... -bench=. -benchmem
```
