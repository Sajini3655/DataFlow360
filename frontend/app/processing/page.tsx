'use client';
import { useCallback, useEffect, useState } from 'react';
import Link from 'next/link';

const API = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';
const names = ['Overview', 'Sources', 'Data Quality', 'Medallion', 'Star Schema', 'Analytics', 'Inventory', 'Processing', 'Airflow'];
const paths = ['/', '/sources', '/quality', '/medallion', '/star-schema', '/analytics', '/inventory', '/processing', '/airflow'];

type Engine = { name: string; available: boolean; error?: string | null };
type Execution = {
  execution_id: number;
  benchmark_id?: string | null;
  engine: string;
  dataset_name: string;
  input_rows: number;
  accepted_rows: number;
  rejected_rows: number;
  duration_seconds: number;
  throughput_rows_per_second: number;
  output_bytes: number;
  state: string;
  resource_metrics?: Record<string, unknown> | null;
  created_at: string;
};
type ProcessingData = { engines: Engine[]; executions: Execution[] };
type Trigger = { dag_run_id: string; state: string; engine: string };
type RunStatus = { state: string };

export default function ProcessingPage() {
  const [data, setData] = useState<ProcessingData | null>(null);
  const [engine, setEngine] = useState('pandas');
  const [run, setRun] = useState<Trigger | null>(null);
  const [runState, setRunState] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');

  const refresh = useCallback(async () => {
    const response = await fetch(`${API}/processing`);
    const body = await response.json();
    if (!response.ok) throw new Error(body.detail || `Processing metrics request failed: ${response.status}`);
    setData(body);
  }, []);

  useEffect(() => {
    void refresh().catch((e: unknown) => setError(e instanceof Error ? e.message : 'Could not load processing metrics'));
    const timer = window.setInterval(() => {
      void refresh().catch((e: unknown) => setError(e instanceof Error ? e.message : 'Could not refresh processing metrics'));
    }, 10000);
    return () => window.clearInterval(timer);
  }, [refresh]);

  useEffect(() => {
    if (!run?.dag_run_id) return;
    let active = true;
    const poll = async () => {
      try {
        const response = await fetch(`${API}/pipeline/runs/${encodeURIComponent(run.dag_run_id)}`);
        const body = await response.json();
        if (!response.ok) throw new Error(body.detail || `Airflow status request failed: ${response.status}`);
        if (active) {
          setRunState((body as RunStatus).state);
          await refresh();
        }
      } catch (e) {
        if (active) setError(e instanceof Error ? e.message : 'Could not retrieve Airflow run status');
      }
    };
    void poll();
    const timer = window.setInterval(() => void poll(), 5000);
    return () => {
      active = false;
      window.clearInterval(timer);
    };
  }, [run?.dag_run_id, refresh]);

  async function trigger(benchmark = false) {
    setBusy(true);
    setError('');
    try {
      const endpoint = benchmark
        ? `${API}/processing/benchmark`
        : `${API}/processing/run?engine=${encodeURIComponent(engine)}`;
      const response = await fetch(endpoint, { method: 'POST' });
      const body = await response.json();
      if (!response.ok) throw new Error(body.detail || `Pipeline trigger failed: ${response.status}`);
      setRun(body);
      setRunState(body.state || 'queued');
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Could not trigger processing');
    } finally {
      setBusy(false);
    }
  }

  const sparkAvailable = data?.engines.some((item) => item.name === 'pyspark' && item.available) ?? false;
  return (
    <div className="shell">
      <aside>
        <div className="brand"><span>◆</span> DataFlow<span className="accent">360</span></div>
        {names.map((name, index) => <Link className={`nav${index === 7 ? ' selected' : ''}`} href={paths[index]} key={name}>{name}</Link>)}
      </aside>
      <main>
        <header>
          <div><small>WORKSPACE / PROCESSING ENGINE</small><h1>Execution and benchmarks</h1>
            <p>Persisted Pandas and PySpark execution statistics</p></div>
          <div className="filters">
            <label>Engine<select value={engine} onChange={(event) => setEngine(event.target.value)}>
              <option value="pandas">Pandas</option>
              <option value="pyspark" disabled={!sparkAvailable}>PySpark{sparkAvailable ? '' : ' (unavailable)'}</option>
            </select></label>
            <button className="primary" disabled={busy || (engine === 'pyspark' && !sparkAvailable)} onClick={() => void trigger()}>
              {busy ? 'Submitting…' : 'Run pipeline'}
            </button>
            <button disabled={busy || !sparkAvailable} onClick={() => void trigger(true)}>Compare engines</button>
          </div>
        </header>
        {error && <div className="error" role="alert">{error}</div>}
        {run && <article><h3>Airflow execution</h3><p>Run ID: <code>{run.dag_run_id}</code></p>
          <p>Engine: {run.engine} · State: {runState || run.state}</p></article>}
        <section className="metrics">
          <article><small>Available engines</small><h2>{data?.engines.filter((item) => item.available).length ?? '—'}</h2></article>
          <article><small>Recorded executions</small><h2>{data?.executions.length ?? '—'}</h2></article>
          <article><small>Latest dataset size</small><h2>{data?.executions[0]?.input_rows.toLocaleString() ?? '—'}</h2></article>
          <article><small>Latest throughput</small><h2>{data?.executions[0] ? `${Number(data.executions[0].throughput_rows_per_second).toLocaleString()} rows/s` : '—'}</h2></article>
        </section>
        <article>
          <h3>Engine availability</h3>
          {data?.engines.map((item) => <p key={item.name}>{item.name}: {item.available ? 'available' : 'unavailable'}{item.error ? ` — ${item.error}` : ''}</p>)}
        </article>
        <article>
          <h3>Persisted processing executions</h3>
          {!data && <p>Loading execution metrics…</p>}
          {data && data.executions.length === 0 && <p>No processing executions have been recorded.</p>}
          {data && data.executions.length > 0 && <table>
            <thead><tr><th>Created</th><th>Engine</th><th>Dataset</th><th>Input</th><th>Accepted</th><th>Rejected</th><th>Duration</th><th>Throughput</th><th>Output</th><th>State</th></tr></thead>
            <tbody>{data.executions.map((item) => <tr key={item.execution_id}>
              <td>{new Date(item.created_at).toLocaleString()}</td><td>{item.engine}</td>
              <td>{item.dataset_name}</td><td>{item.input_rows.toLocaleString()}</td>
              <td>{item.accepted_rows.toLocaleString()}</td><td>{item.rejected_rows.toLocaleString()}</td>
              <td>{Number(item.duration_seconds).toFixed(2)}s</td>
              <td>{Number(item.throughput_rows_per_second).toFixed(2)} rows/s</td>
              <td>{(item.output_bytes / (1024 * 1024)).toFixed(2)} MiB</td><td>{item.state}</td>
            </tr>)}</tbody>
          </table>}
        </article>
      </main>
    </div>
  );
}
