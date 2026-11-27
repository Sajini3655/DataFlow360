'use client';
import { useEffect, useState } from 'react';
import Link from 'next/link';

const API = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';
const names = ['Overview', 'Sources', 'Data Quality', 'Medallion', 'Star Schema', 'Analytics', 'Inventory', 'Processing', 'Airflow'];
const paths = ['/', '/sources', '/quality', '/medallion', '/star-schema', '/analytics', '/inventory', '/processing', '/airflow'];

type Run = { dag_run_id: string; state: string; source: string };
type Task = {
  task_id: string;
  state: string | null;
  start_date?: string | null;
  end_date?: string | null;
  duration?: number | null;
  try_number?: number | null;
  max_tries?: number | null;
  log_url?: string | null;
};
type PipelineStage = {
  stage_name: string;
  state: string;
  rows_read: number;
  rows_written: number;
  error_message?: string | null;
};
type Status = {
  state: string;
  start_date?: string | null;
  end_date?: string | null;
  duration?: number | null;
  scheduled_run?: boolean;
  schedule?: string | null;
  tasks: Task[];
  pipeline_execution?: {
    state: string;
    engine: string;
    accepted: number;
    rejected: number;
    rows_read: number;
    error_message?: string | null;
    stages: PipelineStage[];
    quality?: { overall_score: number; acceptance_rate: number; rejection_rate: number } | null;
  } | null;
};

const terminalTaskStates = new Set(['success', 'failed', 'skipped', 'upstream_failed', 'removed']);

function elapsed(seconds?: number | null, startDate?: string | null, endDate?: string | null) {
  const duration = seconds ?? (
    startDate && endDate ? (Date.parse(endDate) - Date.parse(startDate)) / 1000 : null
  );
  if (duration == null || !Number.isFinite(duration)) return endDate ? '—' : 'In progress';
  return `${Math.floor(duration / 60)}m ${Math.floor(duration % 60)}s`;
}

export default function Airflow() {
  const [busy, setBusy] = useState(false);
  const [run, setRun] = useState<Run | null>(null);
  const [status, setStatus] = useState<Status | null>(null);
  const [error, setError] = useState('');
  const [source, setSource] = useState('csv');
  const [retryingTask, setRetryingTask] = useState('');

  useEffect(() => {
    if (!run?.dag_run_id) return;
    let active = true;
    const poll = async () => {
      try {
        const response = await fetch(`${API}/pipeline/runs/${encodeURIComponent(run.dag_run_id)}`);
        const value = await response.json();
        if (!response.ok) throw new Error(value.detail || `Status request failed: ${response.status}`);
        if (active) {
          setStatus(value);
          setError('');
        }
      } catch (e) {
        if (active) setError(e instanceof Error ? e.message : 'Could not retrieve Airflow run status');
      }
    };
    void poll();
    const timer = window.setInterval(() => void poll(), 3000);
    return () => {
      active = false;
      window.clearInterval(timer);
    };
  }, [run?.dag_run_id]);

  async function start() {
    setBusy(true);
    setError('');
    setStatus(null);
    try {
      const response = await fetch(`${API}/pipeline/run?incremental=true&source=${source}`, { method: 'POST' });
      const body = await response.json();
      if (!response.ok) throw new Error(body.detail || `Trigger failed: ${response.status}`);
      setRun(body);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Could not trigger the Airflow pipeline');
    } finally {
      setBusy(false);
    }
  }

  async function retry(taskId: string) {
    if (!run) return;
    setRetryingTask(taskId);
    setError('');
    try {
      const response = await fetch(
        `${API}/pipeline/runs/${encodeURIComponent(run.dag_run_id)}/tasks/${encodeURIComponent(taskId)}/retry`,
        { method: 'POST' },
      );
      const body = await response.json();
      if (!response.ok) throw new Error(body.detail || `Retry failed: ${response.status}`);
      const updated = await fetch(`${API}/pipeline/runs/${encodeURIComponent(run.dag_run_id)}`);
      const updatedStatus = await updated.json();
      if (!updated.ok) throw new Error(updatedStatus.detail || `Status request failed: ${updated.status}`);
      setStatus(updatedStatus);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Could not retry the Airflow task');
    } finally {
      setRetryingTask('');
    }
  }

  const state = status?.state || run?.state || 'Ready';
  const completedTasks = status?.tasks.filter(
    (task) => task.state !== null && terminalTaskStates.has(task.state),
  ).length || 0;
  return (
    <div className="shell">
      <aside>
        <div className="brand"><span>◆</span> DataFlow<span className="accent">360</span></div>
        {names.map((name, index) => <Link className={`nav${index === 8 ? ' selected' : ''}`} href={paths[index]} key={name}>{name}</Link>)}
      </aside>
      <main>
        <header>
          <div>
            <small>WORKSPACE / AIRFLOW</small>
            <h1>Pipeline orchestration</h1>
            <p>Trigger and monitor real Apache Airflow DAG executions</p>
          </div>
          <div>
            <select aria-label="Source adapter" value={source} onChange={(event) => setSource(event.target.value)}>
              {['csv', 'json', 'rest', 'postgres'].map((item) => <option key={item} value={item}>{item.toUpperCase()}</option>)}
            </select>
            <button className="primary" disabled={busy} onClick={start}>{busy ? 'Submitting…' : '▶ Trigger pipeline'}</button>
          </div>
        </header>
        {error && <div className="error" role="alert">{error}</div>}
        <section className="metrics">
          <article><small>RUN STATE</small><h2>{state}</h2></article>
          <article><small>DAG RUN ID</small><h2 className="compact">{run?.dag_run_id || '—'}</h2></article>
          <article><small>EXECUTION DURATION</small><h2>{elapsed(status?.duration, status?.start_date, status?.end_date)}</h2></article>
          <article><small>TRIGGER</small><h2>{status?.scheduled_run ? 'Scheduled' : run ? 'Manual' : '—'}</h2></article>
        </section>
        <article>
          <h3>Schedule and progress</h3>
          <p>Schedule: {status?.schedule || 'Loading from Airflow'}</p>
          <p>Tasks complete: {completedTasks} / {status?.tasks.length || 0}</p>
          <p>Started: {status?.start_date || '—'} · Finished: {status?.end_date || '—'}</p>
        </article>
        {status && <article>
          <h3>Airflow task instances</h3>
          <table>
            <thead><tr><th>Task</th><th>State</th><th>Attempts</th><th>Duration</th><th>Details / retry</th></tr></thead>
            <tbody>{status.tasks.map((task) => (
              <tr key={task.task_id}>
                <td>{task.task_id}</td>
                <td>{task.state || 'queued'}</td>
                <td>{task.try_number ?? '—'} / {task.max_tries ?? '—'}</td>
                <td>{elapsed(task.duration, task.start_date, task.end_date)}</td>
                <td>
                  {task.log_url && <a href={task.log_url} target="_blank" rel="noreferrer">Task log</a>}
                  {task.state === 'failed' && <button disabled={retryingTask === task.task_id} onClick={() => void retry(task.task_id)}>
                    {retryingTask === task.task_id ? 'Retrying…' : 'Retry failed task'}
                  </button>}
                </td>
              </tr>
            ))}</tbody>
          </table>
        </article>}
        {status?.pipeline_execution && <article>
          <h3>ETL stage timeline</h3>
          <p>Pipeline state: {status.pipeline_execution.state} · Engine: {status.pipeline_execution.engine} · Rows read: {status.pipeline_execution.rows_read} ·
            Accepted: {status.pipeline_execution.accepted} · Rejected: {status.pipeline_execution.rejected}</p>
          {status.pipeline_execution.error_message && <div className="error">{status.pipeline_execution.error_message}</div>}
          {status.pipeline_execution.quality && <p>Quality score: {Number(status.pipeline_execution.quality.overall_score).toFixed(2)}%</p>}
          <table>
            <thead><tr><th>Stage</th><th>State</th><th>Rows read</th><th>Rows written</th><th>Error</th></tr></thead>
            <tbody>{status.pipeline_execution.stages.map((stage) => <tr key={stage.stage_name}>
              <td>{stage.stage_name}</td><td>{stage.state}</td><td>{stage.rows_read}</td><td>{stage.rows_written}</td><td>{stage.error_message || '—'}</td>
            </tr>)}</tbody>
          </table>
        </article>}
        <article>
          <h3>Airflow web UI</h3>
          <p>Live status, task states, and retry requests are retrieved from the Airflow API.</p>
          <a href="http://localhost:8080" target="_blank" rel="noreferrer">Open Airflow UI</a>
        </article>
      </main>
    </div>
  );
}
