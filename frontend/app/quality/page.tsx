'use client';
import { FormEvent, useCallback, useEffect, useState } from 'react';
import Link from 'next/link';

const API = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';
const names = ['Overview', 'Sources', 'Data Quality', 'Medallion', 'Star Schema', 'Analytics', 'Inventory', 'Processing', 'Airflow', 'Cloud Integration'];
const paths = ['/', '/sources', '/quality', '/medallion', '/star-schema', '/analytics', '/inventory', '/processing', '/airflow', '/cloud'];

type Summary = {
  pipeline_run_id: number;
  source_name: string;
  rows_checked: number;
  rows_accepted: number;
  rows_rejected: number;
  acceptance_rate: number;
  rejection_rate: number;
  overall_score: number;
  checked_at: string;
};
type Rule = {
  rule_id: string;
  rule_name: string;
  description: string;
  dimension: string;
  severity: string;
  threshold_percent: number;
  enabled: boolean;
  config: Record<string, unknown>;
  failure_count?: number;
};
type QuarantineRecord = {
  id: number;
  pipeline_run_id: number | null;
  source_name: string;
  source_row_number: number | null;
  record: Record<string, unknown>;
  reason: string;
  rejected_at: string;
  reprocessed_at: string | null;
};
type QualityData = {
  reasons: { reason: string; rows: number }[];
  bronze: number;
  silver: number;
  latest: Summary | null;
  history: Summary[];
  sources: { source_name: string; average_score: number; pipeline_runs: number }[];
  rules: Rule[];
  schema_drift: { drift_id: number; source_name: string; changes: Record<string, unknown>; detected_at: string }[];
  quarantine_pending: number;
};

export default function Quality() {
  const [data, setData] = useState<QualityData | null>(null);
  const [rules, setRules] = useState<Rule[]>([]);
  const [drafts, setDrafts] = useState<Record<string, { enabled: boolean; threshold: string; config: string }>>({});
  const [quarantine, setQuarantine] = useState<QuarantineRecord[]>([]);
  const [sourceFilter, setSourceFilter] = useState('');
  const [reasonFilter, setReasonFilter] = useState('');
  const [selected, setSelected] = useState<QuarantineRecord | null>(null);
  const [correction, setCorrection] = useState('');
  const [error, setError] = useState('');
  const [notice, setNotice] = useState('');
  const [busy, setBusy] = useState(false);

  const refresh = useCallback(async () => {
    const query = new URLSearchParams({ limit: '100' });
    if (sourceFilter) query.set('source_name', sourceFilter);
    if (reasonFilter) query.set('reason', reasonFilter);
    const [qualityResponse, rulesResponse, quarantineResponse] = await Promise.all([
      fetch(`${API}/quality`),
      fetch(`${API}/quality/rules`),
      fetch(`${API}/quality/quarantine?${query.toString()}`),
    ]);
    const [qualityBody, rulesBody, quarantineBody] = await Promise.all([
      qualityResponse.json(),
      rulesResponse.json(),
      quarantineResponse.json(),
    ]);
    if (!qualityResponse.ok) throw new Error(qualityBody.detail || `Quality request failed: ${qualityResponse.status}`);
    if (!rulesResponse.ok) throw new Error(rulesBody.detail || `Rule request failed: ${rulesResponse.status}`);
    if (!quarantineResponse.ok) throw new Error(quarantineBody.detail || `Quarantine request failed: ${quarantineResponse.status}`);
    setData(qualityBody);
    setRules(rulesBody);
    setDrafts(Object.fromEntries(rulesBody.map((rule: Rule) => [
      rule.rule_id,
      {
        enabled: rule.enabled,
        threshold: String(rule.threshold_percent),
        config: JSON.stringify(rule.config ?? {}, null, 2),
      },
    ])));
    setQuarantine(quarantineBody);
  }, [sourceFilter, reasonFilter]);

  useEffect(() => {
    void refresh().catch((e: unknown) => setError(e instanceof Error ? e.message : 'Could not load quality data'));
  }, [refresh]);

  async function saveRule(rule: Rule) {
    const draft = drafts[rule.rule_id];
    if (!draft) return;
    setError('');
    setNotice('');
    try {
      const threshold = Number(draft.threshold);
      if (!Number.isFinite(threshold) || threshold < 0 || threshold > 100) {
        throw new Error('Threshold must be between 0 and 100 percent');
      }
      const config: unknown = JSON.parse(draft.config);
      if (!config || typeof config !== 'object' || Array.isArray(config)) {
        throw new Error('Rule configuration must be a JSON object');
      }
      const response = await fetch(`${API}/quality/rules/${encodeURIComponent(rule.rule_id)}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ enabled: draft.enabled, threshold_percent: threshold, config }),
      });
      const body = await response.json();
      if (!response.ok) throw new Error(body.detail || `Rule update failed: ${response.status}`);
      setNotice(`Saved ${rule.rule_name}`);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Could not save the quality rule');
    }
  }

  function editRecord(record: QuarantineRecord) {
    setSelected(record);
    setCorrection(JSON.stringify(record.record, null, 2));
    setError('');
    setNotice('');
  }

  async function reprocess(event: FormEvent) {
    event.preventDefault();
    if (!selected) return;
    setBusy(true);
    setError('');
    setNotice('');
    try {
      const parsed = JSON.parse(correction) as Record<string, unknown>;
      const response = await fetch(`${API}/quality/quarantine/${selected.id}/reprocess`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(parsed),
      });
      const body = await response.json();
      if (!response.ok) throw new Error(body.detail || `Reprocessing failed: ${response.status}`);
      setSelected(null);
      setNotice(`Record reprocessed in pipeline run ${body.pipeline_run_id}`);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Could not reprocess the corrected record');
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="shell">
      <aside>
        <div className="brand"><span>◆</span> DataFlow<span className="accent">360</span></div>
        {names.map((name, index) => <Link className={`nav${index === 2 ? ' selected' : ''}`} href={paths[index]} key={name}>{name}</Link>)}
      </aside>
      <main>
        <header><div><small>WORKSPACE / DATA QUALITY</small><h1>Validation and recovery</h1><p>Persisted quality scores, rules, drift, and quarantined records</p></div></header>
        {error && <div className="error" role="alert">{error}</div>}
        {notice && <p role="status">{notice}</p>}
        {!data && !error && <p>Loading quality metrics…</p>}
        {data && <>
          <section className="metrics">
            <article><small>OVERALL QUALITY SCORE</small><h2>{data.latest ? `${Number(data.latest.overall_score).toFixed(2)}%` : 'No runs yet'}</h2></article>
            <article><small>ACCEPTANCE RATE</small><h2>{data.latest ? `${Number(data.latest.acceptance_rate).toFixed(2)}%` : '—'}</h2></article>
            <article><small>REJECTION RATE</small><h2>{data.latest ? `${Number(data.latest.rejection_rate).toFixed(2)}%` : '—'}</h2></article>
            <article><small>PENDING QUARANTINE</small><h2>{data.quarantine_pending}</h2></article>
          </section>
          <section className="metrics">
            <article><small>BRONZE ROWS</small><h2>{data.bronze}</h2></article>
            <article><small>SILVER ROWS</small><h2>{data.silver}</h2></article>
            <article><small>LAST CHECKED</small><h2 className="compact">{data.latest ? new Date(data.latest.checked_at).toLocaleString() : '—'}</h2></article>
          </section>
          <article>
            <h3>Quality score history</h3>
            {data.history.length === 0 ? <p>No persisted quality runs yet.</p> : <table>
              <thead><tr><th>Checked</th><th>Source</th><th>Rows</th><th>Accepted</th><th>Rejected</th><th>Score</th></tr></thead>
              <tbody>{data.history.map((item) => <tr key={item.pipeline_run_id}>
                <td>{new Date(item.checked_at).toLocaleString()}</td><td>{item.source_name}</td>
                <td>{item.rows_checked}</td><td>{item.rows_accepted}</td><td>{item.rows_rejected}</td>
                <td><progress max="100" value={Number(item.overall_score)} /> {Number(item.overall_score).toFixed(2)}%</td>
              </tr>)}</tbody>
            </table>}
          </article>
          <article>
            <h3>Quality score by source</h3>
            {data.sources.length === 0 ? <p>No source quality history yet.</p> : <table>
              <thead><tr><th>Source</th><th>Average score</th><th>Runs</th></tr></thead>
              <tbody>{data.sources.map((item) => <tr key={item.source_name}>
                <td>{item.source_name}</td><td><progress max="100" value={Number(item.average_score)} /> {Number(item.average_score).toFixed(2)}%</td><td>{item.pipeline_runs}</td>
              </tr>)}</tbody>
            </table>}
          </article>
          <article>
            <h3>Configurable validation rules</h3>
            <table>
              <thead><tr><th>Rule</th><th>Dimension / severity</th><th>Failures</th><th>Enabled</th><th>Allowed failure %</th><th>Parameters (JSON)</th><th>Action</th></tr></thead>
              <tbody>{rules.map((rule) => <tr key={rule.rule_id}>
                <td title={rule.description}>{rule.rule_name}</td>
                <td>{rule.dimension} / {rule.severity}</td>
                <td>{rule.failure_count ?? 0}</td>
                <td><input aria-label={`Enable ${rule.rule_name}`} type="checkbox" checked={drafts[rule.rule_id]?.enabled ?? rule.enabled}
                  onChange={(event) => setDrafts((value) => ({ ...value, [rule.rule_id]: { ...value[rule.rule_id], enabled: event.target.checked } }))} /></td>
                <td><input aria-label={`Threshold for ${rule.rule_name}`} type="number" min="0" max="100" step="0.1"
                  value={drafts[rule.rule_id]?.threshold ?? rule.threshold_percent}
                  onChange={(event) => setDrafts((value) => ({ ...value, [rule.rule_id]: { ...value[rule.rule_id], threshold: event.target.value } }))} /></td>
                <td><textarea aria-label={`Configuration for ${rule.rule_name}`} rows={3}
                  value={drafts[rule.rule_id]?.config ?? JSON.stringify(rule.config ?? {})}
                  onChange={(event) => setDrafts((value) => ({ ...value, [rule.rule_id]: { ...value[rule.rule_id], config: event.target.value } }))} /></td>
                <td><button onClick={() => void saveRule(rule)}>Save</button></td>
              </tr>)}</tbody>
            </table>
          </article>
          <article>
            <h3>Rejection breakdown</h3>
            {data.reasons.length === 0 ? <p>No rejected records.</p> : <table>
              <thead><tr><th>Reason</th><th>Rows</th></tr></thead>
              <tbody>{data.reasons.map((item) => <tr key={item.reason}><td>{item.reason}</td><td>{item.rows}</td></tr>)}</tbody>
            </table>}
          </article>
          <article>
            <h3>Schema drift events</h3>
            {data.schema_drift.length === 0 ? <p>No schema drift events recorded.</p> : <table>
              <thead><tr><th>Detected</th><th>Source</th><th>Changes</th></tr></thead>
              <tbody>{data.schema_drift.map((item) => <tr key={item.drift_id}>
                <td>{new Date(item.detected_at).toLocaleString()}</td><td>{item.source_name}</td><td><code>{JSON.stringify(item.changes)}</code></td>
              </tr>)}</tbody>
            </table>}
          </article>
          <article>
            <h3>Quarantine</h3>
            <div>
              <label>Source <select value={sourceFilter} onChange={(event) => setSourceFilter(event.target.value)}>
                <option value="">All sources</option>
                {data.sources.map((item) => <option key={item.source_name} value={item.source_name}>{item.source_name}</option>)}
              </select></label>
              <label>Reason <select value={reasonFilter} onChange={(event) => setReasonFilter(event.target.value)}>
                <option value="">All reasons</option>
                {data.reasons.map((item) => <option key={item.reason} value={item.reason}>{item.reason}</option>)}
              </select></label>
              <a href={`${API}/quality/quarantine/export.csv${sourceFilter ? `?source_name=${encodeURIComponent(sourceFilter)}` : ''}`}>Export CSV</a>
            </div>
            {quarantine.length === 0 ? <p>No pending quarantined rows match these filters.</p> : <table>
              <thead><tr><th>Source row</th><th>Source</th><th>Reason</th><th>Rejected</th><th>Correction</th></tr></thead>
              <tbody>{quarantine.map((item) => <tr key={item.id}>
                <td>{item.source_row_number ?? '—'}</td><td>{item.source_name}</td><td>{item.reason}</td>
                <td>{new Date(item.rejected_at).toLocaleString()}</td>
                <td><button onClick={() => editRecord(item)}>Correct / reprocess</button></td>
              </tr>)}</tbody>
            </table>}
            {selected && <form onSubmit={reprocess}>
              <h4>Correct record #{selected.id}</h4>
              <textarea aria-label="Corrected record JSON" rows={10} value={correction} onChange={(event) => setCorrection(event.target.value)} />
              <button className="primary" type="submit" disabled={busy}>{busy ? 'Reprocessing…' : 'Validate and reprocess'}</button>
              <button type="button" onClick={() => setSelected(null)}>Cancel</button>
            </form>}
          </article>
        </>}
      </main>
    </div>
  );
}
