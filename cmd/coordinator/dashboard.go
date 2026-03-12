package main

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/aakashkolli/dag-scheduler/internal/proto/scheduler"
	"github.com/aakashkolli/dag-scheduler/internal/store"
)

// dashboardServer exposes REST endpoints and serves the HTML dashboard.
type dashboardServer struct {
	store *store.StateStore
}

func newDashboardServer(s *store.StateStore) *dashboardServer {
	return &dashboardServer{store: s}
}

func (d *dashboardServer) register(mux *http.ServeMux) {
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" || r.URL.Path == "/dashboard" {
			d.handleDashboard(w, r)
		} else {
			http.NotFound(w, r)
		}
	})
	mux.HandleFunc("/api/workflows", d.handleAPIWorkflowList)
	mux.HandleFunc("/api/workflows/", d.handleAPIWorkflowDetail)
	mux.HandleFunc("/api/workers", d.handleAPIWorkers)
}

// ── API response types ────────────────────────────────────────────────────────

type workflowSummary struct {
	ID            string `json:"id"`
	State         string `json:"state"`
	SubmittedAtMs int64  `json:"submitted_at_ms"`
	CompletedAtMs int64  `json:"completed_at_ms"`
	DurationMs    int64  `json:"duration_ms"`
	TaskCount     int    `json:"task_count"`
	DoneCount     int    `json:"done_count"`
	FailedCount   int    `json:"failed_count"`
	FailurePolicy string `json:"failure_policy"`
}

type workflowDetail struct {
	workflowSummary
	Tasks []taskDetail `json:"tasks"`
}

type taskDetail struct {
	ID           string   `json:"id"`
	State        string   `json:"state"`
	Command      string   `json:"command"`
	Dependencies []string `json:"dependencies"`
	AttemptCount int32    `json:"attempt_count"`
	MaxRetries   int32    `json:"max_retries"`
	Error        string   `json:"error,omitempty"`
	Output       string   `json:"output,omitempty"`
}

type workerSummary struct {
	ID               string   `json:"id"`
	LastHeartbeatMs  int64    `json:"last_heartbeat_ms"`
	SecondsSince     int64    `json:"seconds_since"`
	RunningTaskIDs   []string `json:"running_task_ids"`
	Status           string   `json:"status"`
}

// ── Handlers ──────────────────────────────────────────────────────────────────

func jsonOK(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(v)
}

func (d *dashboardServer) handleAPIWorkflowList(w http.ResponseWriter, _ *http.Request) {
	states, err := d.store.GetAllWorkflowStates()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	now := time.Now().UnixMilli()
	result := make([]workflowSummary, 0, len(states))
	for _, wf := range states {
		tasks, _ := d.store.GetTaskStatusesForWorkflow(wf.WorkflowID)
		done, failed := countTaskDoneAndFailed(tasks)
		dur := computeDuration(wf, now)
		result = append(result, workflowSummary{
			ID:            wf.WorkflowID,
			State:         wf.State.String(),
			SubmittedAtMs: wf.SubmittedAtMs,
			CompletedAtMs: wf.CompletedAtMs,
			DurationMs:    dur,
			TaskCount:     len(tasks),
			DoneCount:     done,
			FailedCount:   failed,
			FailurePolicy: "", // not stored in WorkflowStateRecord
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].SubmittedAtMs > result[j].SubmittedAtMs
	})

	jsonOK(w, result)
}

func (d *dashboardServer) handleAPIWorkflowDetail(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/workflows/")
	if id == "" {
		http.Error(w, "missing workflow id", 400)
		return
	}

	wfState, err := d.store.GetWorkflowState(id)
	if err != nil {
		http.Error(w, err.Error(), 404)
		return
	}

	taskStatuses, err := d.store.GetTaskStatusesForWorkflow(id)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	// Index task statuses by ID
	statusByID := map[string]*scheduler.TaskStatus{}
	for _, ts := range taskStatuses {
		statusByID[ts.TaskId] = ts
	}

	// Get workflow spec for commands + deps
	spec, _ := d.store.GetWorkflow(id)

	var tasks []taskDetail
	policy := ""
	if spec != nil {
		policy = spec.FailurePolicy.String()
		for _, t := range spec.Tasks {
			td := taskDetail{
				ID:           t.TaskId,
				Command:      t.Command,
				Dependencies: t.Dependencies,
				MaxRetries:   t.MaxRetries,
			}
			if st, ok := statusByID[t.TaskId]; ok {
				td.State = st.State.String()
				td.AttemptCount = st.AttemptCount
				td.Error = st.Error
				td.Output = trimOutput(st.Output, 512)
			}
			if td.Dependencies == nil {
				td.Dependencies = []string{}
			}
			tasks = append(tasks, td)
		}
	} else {
		// Spec not available; use statuses only
		for _, ts := range taskStatuses {
			tasks = append(tasks, taskDetail{
				ID:           ts.TaskId,
				State:        ts.State.String(),
				AttemptCount: ts.AttemptCount,
				Error:        ts.Error,
				Output:       trimOutput(ts.Output, 512),
				Dependencies: []string{},
			})
		}
	}

	now := time.Now().UnixMilli()
	done, failed := countTaskDoneAndFailed(taskStatuses)
	detail := workflowDetail{
		workflowSummary: workflowSummary{
			ID:            wfState.WorkflowID,
			State:         wfState.State.String(),
			SubmittedAtMs: wfState.SubmittedAtMs,
			CompletedAtMs: wfState.CompletedAtMs,
			DurationMs:    computeDuration(wfState, now),
			TaskCount:     len(taskStatuses),
			DoneCount:     done,
			FailedCount:   failed,
			FailurePolicy: policy,
		},
		Tasks: tasks,
	}

	jsonOK(w, detail)
}

func (d *dashboardServer) handleAPIWorkers(w http.ResponseWriter, _ *http.Request) {
	workers, err := d.store.GetAllWorkers()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	deadThresholdMs := int64(15 * time.Second / time.Millisecond)
	now := time.Now().UnixMilli()

	result := make([]workerSummary, 0, len(workers))
	for _, wk := range workers {
		age := (now - wk.LastHeartbeat) / 1000
		status := "alive"
		if now-wk.LastHeartbeat > deadThresholdMs {
			status = "dead"
		}
		running := wk.RunningTaskIDs
		if running == nil {
			running = []string{}
		}
		result = append(result, workerSummary{
			ID:             wk.WorkerID,
			LastHeartbeatMs: wk.LastHeartbeat,
			SecondsSince:   age,
			RunningTaskIDs: running,
			Status:         status,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].LastHeartbeatMs > result[j].LastHeartbeatMs
	})

	jsonOK(w, result)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func countTaskDoneAndFailed(tasks []*scheduler.TaskStatus) (done, failed int) {
	for _, t := range tasks {
		switch t.State {
		case scheduler.TaskState_TASK_COMPLETE, scheduler.TaskState_TASK_SKIPPED:
			done++
		case scheduler.TaskState_TASK_FAILED:
			failed++
		}
	}
	return
}

func computeDuration(wf *store.WorkflowStateRecord, nowMs int64) int64 {
	if wf.CompletedAtMs > 0 {
		return wf.CompletedAtMs - wf.SubmittedAtMs
	}
	if wf.SubmittedAtMs > 0 {
		return nowMs - wf.SubmittedAtMs
	}
	return 0
}

func trimOutput(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}

// ── Dashboard HTML ────────────────────────────────────────────────────────────

func (d *dashboardServer) handleDashboard(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(dashboardHTML))
}

const dashboardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>dag-scheduler</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#000;
  --surface:#080808;
  --border:#1c1c1c;
  --text:#f0f0f0;
  --muted:#555;
  --dim:#333;
  --font:'JetBrains Mono','Fira Mono','Cascadia Code','Courier New',monospace;
  --size:13px;
}
html,body{height:100%;background:var(--bg);color:var(--text);font-family:var(--font);font-size:var(--size);line-height:1.6}
a{color:inherit;text-decoration:none}
button{font-family:var(--font);font-size:var(--size);cursor:pointer}

/* ── layout ── */
#app{display:flex;flex-direction:column;min-height:100vh}

/* ── topbar ── */
#topbar{
  display:flex;align-items:center;justify-content:space-between;
  padding:14px 24px;
  border-bottom:1px solid var(--border);
  position:sticky;top:0;background:var(--bg);z-index:10;
}
#topbar-left{display:flex;align-items:center;gap:20px}
.brand{font-size:13px;font-weight:700;letter-spacing:.12em;text-transform:uppercase}
.sep{color:var(--dim)}
#topbar-right{display:flex;align-items:center;gap:16px;color:var(--muted);font-size:12px}
#refresh-status{display:flex;align-items:center;gap:6px}
.dot{
  width:6px;height:6px;border-radius:50%;
  background:var(--dim);
  display:inline-block;
  transition:background .15s;
}
.dot.active{background:#fff}
#clock{font-variant-numeric:tabular-nums}

/* ── stats bar ── */
#statsbar{
  display:flex;gap:0;
  border-bottom:1px solid var(--border);
}
.stat{
  flex:1;padding:14px 24px;
  border-right:1px solid var(--border);
}
.stat:last-child{border-right:none}
.stat-num{font-size:22px;font-weight:700;letter-spacing:-.02em;line-height:1}
.stat-label{font-size:10px;text-transform:uppercase;letter-spacing:.1em;color:var(--muted);margin-top:4px}

/* ── section ── */
.section{padding:20px 24px;border-bottom:1px solid var(--border)}
.section-hdr{
  font-size:10px;text-transform:uppercase;letter-spacing:.12em;
  color:var(--muted);margin-bottom:14px;
  display:flex;align-items:center;justify-content:space-between;
}

/* ── table ── */
.tbl{width:100%;border-collapse:collapse}
.tbl th{
  text-align:left;padding:0 16px 8px 0;
  font-size:10px;font-weight:400;
  text-transform:uppercase;letter-spacing:.1em;color:var(--muted);
  border-bottom:1px solid var(--border);
}
.tbl td{padding:10px 16px 10px 0;vertical-align:top;border-bottom:1px solid #0f0f0f}
.tbl tbody tr{cursor:pointer}
.tbl tbody tr:hover td{background:#080808}
.tbl tbody tr.expanded td{background:#080808}

/* ── task subtable ── */
.task-panel{
  background:var(--surface);
  border-bottom:1px solid var(--border);
}
.task-panel td{padding:12px 24px}
.subtbl{width:100%;border-collapse:collapse}
.subtbl td{
  padding:5px 16px 5px 0;
  font-size:12px;
  border-bottom:1px solid #0d0d0d;
  vertical-align:top;
}
.subtbl tr:last-child td{border-bottom:none}
.td-id{width:180px;font-weight:700}
.td-state{width:140px}
.td-deps{width:200px;color:var(--muted)}
.td-cmd{color:var(--muted);font-size:11px;white-space:pre-wrap;word-break:break-all}
.td-err{color:#aaa;font-size:11px;white-space:pre-wrap;word-break:break-all}

/* ── state badges ── */
.s-running{color:#fff}
.s-complete{color:var(--muted)}
.s-failed{color:#fff;font-weight:700}
.s-queued{color:var(--dim)}
.s-pending{color:var(--dim)}
.s-skipped{color:var(--dim)}
.s-cancelled{color:var(--dim)}
.s-unknown{color:var(--muted)}

/* ── running pulse ── */
@keyframes blink{0%,100%{opacity:1}50%{opacity:.2}}
.blink{animation:blink 1.4s ease-in-out infinite;display:inline-block}

/* ── progress bar ── */
.prog-wrap{width:80px;height:4px;background:var(--dim);border-radius:0;display:inline-block;vertical-align:middle}
.prog-fill{height:100%;background:#fff;transition:width .4s}

/* ── worker list ── */
.worker-row{
  display:grid;grid-template-columns:220px 80px 100px 1fr;
  gap:16px;padding:9px 0;
  border-bottom:1px solid #0f0f0f;
  font-size:13px;align-items:start;
}
.worker-row:last-child{border-bottom:none}
.w-id{color:var(--muted);font-size:12px;word-break:break-all}
.w-status-alive{color:#fff}
.w-status-dead{color:var(--muted)}
.w-tasks{color:var(--muted);font-size:12px}

/* ── empty ── */
.empty{color:var(--muted);padding:20px 0;font-size:12px}

/* ── error banner ── */
#error-banner{
  display:none;
  padding:10px 24px;
  background:#0f0f0f;border-bottom:1px solid var(--border);
  color:var(--muted);font-size:12px;
}
</style>
</head>
<body>
<div id="app">
  <div id="topbar">
    <div id="topbar-left">
      <span class="brand">dag-scheduler</span>
      <span class="sep">/</span>
      <span style="color:var(--muted);font-size:12px" id="coordinator-addr">localhost:50051</span>
    </div>
    <div id="topbar-right">
      <span id="refresh-status"><span class="dot" id="refresh-dot"></span><span id="refresh-label">refreshing every 5s</span></span>
      <span class="sep">·</span>
      <span id="clock"></span>
    </div>
  </div>

  <div id="error-banner" id="ebanner"></div>

  <div id="statsbar">
    <div class="stat"><div class="stat-num" id="s-total">—</div><div class="stat-label">Workflows</div></div>
    <div class="stat"><div class="stat-num" id="s-running">—</div><div class="stat-label">Running</div></div>
    <div class="stat"><div class="stat-num" id="s-complete">—</div><div class="stat-label">Complete</div></div>
    <div class="stat"><div class="stat-num" id="s-failed">—</div><div class="stat-label">Failed</div></div>
    <div class="stat"><div class="stat-num" id="s-workers">—</div><div class="stat-label">Workers</div></div>
  </div>

  <div class="section">
    <div class="section-hdr"><span>Workflows</span></div>
    <table class="tbl" id="wf-table">
      <thead>
        <tr>
          <th style="width:220px">Workflow ID</th>
          <th style="width:160px">State</th>
          <th style="width:160px">Submitted</th>
          <th style="width:100px">Duration</th>
          <th style="width:120px">Tasks</th>
          <th>Policy</th>
        </tr>
      </thead>
      <tbody id="wf-body"></tbody>
    </table>
    <div class="empty" id="wf-empty" style="display:none">No workflows submitted yet.</div>
  </div>

  <div class="section">
    <div class="section-hdr"><span>Workers</span></div>
    <div id="workers-list"></div>
    <div class="empty" id="workers-empty" style="display:none">No workers connected.</div>
  </div>
</div>

<script>
'use strict';

const API = '';
let expandedID = null;
let workflows = [];
let workers = [];

// ── clock ──────────────────────────────────────────────────────────────────
function updateClock() {
  document.getElementById('clock').textContent = new Date().toLocaleTimeString('en-US', {hour12:false});
}
setInterval(updateClock, 1000);
updateClock();

// ── fetch helpers ──────────────────────────────────────────────────────────
async function fetchJSON(path) {
  const r = await fetch(API + path);
  if (!r.ok) throw new Error(r.status + ' ' + r.statusText);
  return r.json();
}

// ── format helpers ─────────────────────────────────────────────────────────
function fmtDuration(ms) {
  if (!ms || ms <= 0) return '—';
  const s = Math.floor(ms / 1000);
  const m = Math.floor(s / 60);
  const h = Math.floor(m / 60);
  if (h > 0) return h + 'h ' + (m % 60) + 'm';
  if (m > 0) return m + 'm ' + (s % 60) + 's';
  return s + 's';
}

function fmtTime(ms) {
  if (!ms) return '—';
  const d = new Date(ms);
  return d.toLocaleTimeString('en-US', {hour12:false}) + ' ' +
    d.toLocaleDateString('en-US', {month:'2-digit', day:'2-digit'});
}

function fmtAgo(s) {
  if (s < 60) return s + 's ago';
  if (s < 3600) return Math.floor(s/60) + 'm ago';
  return Math.floor(s/3600) + 'h ago';
}

function stateClass(s) {
  if (!s) return 's-unknown';
  const l = s.toLowerCase();
  if (l.includes('running')) return 's-running';
  if (l.includes('complete')) return 's-complete';
  if (l.includes('failed')) return 's-failed';
  if (l.includes('queued')) return 's-queued';
  if (l.includes('pending')) return 's-pending';
  if (l.includes('skipped')) return 's-cancelled';
  if (l.includes('cancel')) return 's-cancelled';
  if (l.includes('unknown')) return 's-unknown';
  return 's-unknown';
}

function stateLabel(s) {
  if (!s) return '—';
  const l = s.toLowerCase();
  if (l.includes('running')) return '<span class="blink">▶</span> RUNNING';
  if (l.includes('complete')) return '✓ DONE';
  if (l.includes('failed')) return '✗ FAILED';
  if (l.includes('queued')) return '· QUEUED';
  if (l.includes('pending')) return '· PENDING';
  if (l.includes('skipped')) return '⊘ SKIPPED';
  if (l.includes('cancel')) return '⊘ CANCELLED';
  if (l.includes('unknown')) return '? UNKNOWN';
  return s;
}

function taskStateLabel(s) {
  if (!s) return '—';
  const l = s.toLowerCase();
  if (l.includes('running')) return '<span class="blink">▶</span> running';
  if (l.includes('complete')) return '✓ done';
  if (l.includes('failed')) return '✗ failed';
  if (l.includes('queued')) return '· queued';
  if (l.includes('pending')) return '· pending';
  if (l.includes('skipped')) return '⊘ skipped';
  if (l.includes('cancel')) return '⊘ cancelled';
  if (l.includes('unknown')) return '? unknown';
  return s;
}

function policyLabel(p) {
  if (!p) return '—';
  const l = p.toLowerCase();
  if (l.includes('fail_fast') || l === 'fail_fast') return 'FAIL_FAST';
  if (l.includes('skip')) return 'SKIP_DOWNSTREAM';
  if (l.includes('continue')) return 'CONTINUE_IND.';
  return p.replace('FAILURE_POLICY_','');
}

// ── render workflows ────────────────────────────────────────────────────────
function renderWorkflows() {
  const tbody = document.getElementById('wf-body');
  const empty = document.getElementById('wf-empty');

  if (!workflows.length) {
    tbody.innerHTML = '';
    empty.style.display = '';
    return;
  }
  empty.style.display = 'none';

  // Count stats
  let running=0, complete=0, failed=0;
  for (const wf of workflows) {
    const l = (wf.state||'').toLowerCase();
    if (l.includes('running')) running++;
    else if (l.includes('complete')) complete++;
    else if (l.includes('failed')) failed++;
  }
  document.getElementById('s-total').textContent = workflows.length;
  document.getElementById('s-running').textContent = running;
  document.getElementById('s-complete').textContent = complete;
  document.getElementById('s-failed').textContent = failed;

  let html = '';
  for (const wf of workflows) {
    const sc = stateClass(wf.state);
    const sl = stateLabel(wf.state);
    const pct = wf.task_count > 0 ? Math.round((wf.done_count / wf.task_count) * 100) : 0;
    const isExp = expandedID === wf.id;
    html += '<tr data-id="' + esc(wf.id) + '"' + (isExp ? ' class="expanded"' : '') + '>';
    html += '<td style="font-weight:700">' + esc(wf.id) + '</td>';
    html += '<td class="' + sc + '">' + sl + '</td>';
    html += '<td style="color:var(--muted);font-size:12px">' + fmtTime(wf.submitted_at_ms) + '</td>';
    html += '<td style="font-variant-numeric:tabular-nums">' + fmtDuration(wf.duration_ms) + '</td>';
    html += '<td>';
    if (wf.task_count > 0) {
      html += '<span style="color:var(--muted);font-size:12px">' + wf.done_count + '/' + wf.task_count + '</span> ';
      html += '<span class="prog-wrap"><span class="prog-fill" style="width:' + pct + '%"></span></span>';
    } else {
      html += '<span style="color:var(--muted)">—</span>';
    }
    html += '</td>';
    html += '<td style="color:var(--muted);font-size:11px">' + policyLabel(wf.failure_policy) + '</td>';
    html += '</tr>';

    if (isExp) {
      html += '<tr class="task-panel"><td colspan="6"><div id="task-detail-' + esc(wf.id) + '">Loading…</div></td></tr>';
    }
  }
  tbody.innerHTML = html;

  // Attach click handlers
  for (const row of tbody.querySelectorAll('tr[data-id]')) {
    row.addEventListener('click', () => {
      const id = row.dataset.id;
      if (expandedID === id) {
        expandedID = null;
        renderWorkflows();
      } else {
        expandedID = id;
        renderWorkflows();
        loadTaskDetail(id);
      }
    });
  }

  // Restore task detail if expanded
  if (expandedID) {
    const el = document.getElementById('task-detail-' + expandedID);
    if (el && el.textContent === 'Loading…') {
      loadTaskDetail(expandedID);
    }
  }
}

async function loadTaskDetail(id) {
  const el = document.getElementById('task-detail-' + id);
  if (!el) return;
  try {
    const detail = await fetchJSON('/api/workflows/' + encodeURIComponent(id));
    renderTaskDetail(el, detail);
  } catch(e) {
    el.textContent = 'Error: ' + e.message;
  }
}

function renderTaskDetail(el, detail) {
  if (!detail.tasks || !detail.tasks.length) {
    el.innerHTML = '<span style="color:var(--muted);font-size:12px">No task data available.</span>';
    return;
  }
  let html = '<table class="subtbl"><thead><tr>';
  html += '<td class="td-id" style="color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.1em;padding-bottom:6px">Task</td>';
  html += '<td class="td-state" style="color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.1em;padding-bottom:6px">State</td>';
  html += '<td class="td-deps" style="color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.1em;padding-bottom:6px">Deps</td>';
  html += '<td style="color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:.1em;padding-bottom:6px">Command / Error</td>';
  html += '</tr></thead><tbody>';
  for (const t of detail.tasks) {
    const sc = stateClass(t.state);
    const sl = taskStateLabel(t.state);
    const deps = t.dependencies && t.dependencies.length ? t.dependencies.join(', ') : '—';
    const retries = t.attempt_count > 1 ? ' <span style="color:var(--muted);font-size:10px">[' + t.attempt_count + ' attempts]</span>' : '';
    html += '<tr>';
    html += '<td class="td-id">' + esc(t.id) + retries + '</td>';
    html += '<td class="td-state ' + sc + '">' + sl + '</td>';
    html += '<td class="td-deps">' + esc(deps) + '</td>';
    html += '<td>';
    if (t.error) {
      html += '<span class="td-err">✗ ' + esc(t.error) + '</span>';
    } else if (t.command) {
      html += '<span class="td-cmd">$ ' + esc(t.command) + '</span>';
    } else {
      html += '<span style="color:var(--dim)">—</span>';
    }
    html += '</td>';
    html += '</tr>';
  }
  html += '</tbody></table>';
  el.innerHTML = html;
}

// ── render workers ─────────────────────────────────────────────────────────
function renderWorkers() {
  const list = document.getElementById('workers-list');
  const empty = document.getElementById('workers-empty');

  document.getElementById('s-workers').textContent = workers.filter(w => w.status === 'alive').length;

  if (!workers.length) {
    list.innerHTML = '';
    empty.style.display = '';
    return;
  }
  empty.style.display = 'none';

  let html = '';
  for (const wk of workers) {
    const alive = wk.status === 'alive';
    html += '<div class="worker-row">';
    html += '<div class="w-id">' + esc(wk.id) + '</div>';
    html += '<div class="' + (alive ? 'w-status-alive' : 'w-status-dead') + '">' +
      (alive ? '<span class="blink">●</span> alive' : '○ dead') + '</div>';
    html += '<div style="color:var(--muted);font-size:12px">' + fmtAgo(wk.seconds_since) + '</div>';
    html += '<div class="w-tasks">' +
      (wk.running_task_ids && wk.running_task_ids.length ? wk.running_task_ids.map(esc).join(', ') : '—') +
      '</div>';
    html += '</div>';
  }
  list.innerHTML = html;
}

// ── refresh ────────────────────────────────────────────────────────────────
const dot = document.getElementById('refresh-dot');

async function refresh() {
  dot.classList.add('active');
  try {
    [workflows, workers] = await Promise.all([
      fetchJSON('/api/workflows'),
      fetchJSON('/api/workers'),
    ]);
    workflows = workflows || [];
    workers = workers || [];
    renderWorkflows();
    renderWorkers();
    // Refresh task detail if expanded
    if (expandedID) {
      const el = document.getElementById('task-detail-' + expandedID);
      if (el) loadTaskDetail(expandedID);
    }
  } catch(e) {
    console.error('refresh error:', e);
  } finally {
    setTimeout(() => dot.classList.remove('active'), 200);
  }
}

function esc(s) {
  if (s == null) return '';
  return String(s)
    .replace(/&/g,'&amp;')
    .replace(/</g,'&lt;')
    .replace(/>/g,'&gt;')
    .replace(/"/g,'&quot;');
}

refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>`
