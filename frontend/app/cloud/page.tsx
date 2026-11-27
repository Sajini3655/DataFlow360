'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';

const API = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';
const names = ['Overview', 'Sources', 'Data Quality', 'Medallion', 'Star Schema', 'Analytics', 'Inventory', 'Processing', 'Airflow', 'Cloud Integration', 'Business Intelligence'];
const paths = ['/', '/sources', '/quality', '/medallion', '/star-schema', '/analytics', '/inventory', '/processing', '/airflow', '/cloud', '/business-intelligence'];
const layers = ['bronze', 'silver', 'gold'] as const;

type CloudStatus = {
  execution_modes: { mode: string; available: boolean }[];
  storage: {
    mode: string;
    configured: boolean;
    account_host: string | null;
    authentication: string;
    sdk_available: boolean | null;
    containers: Record<string, string>;
    configuration_error: string | null;
    connection: { state: string; message: string; tested_at?: string | null };
    last_sync: { state: string; message: string; finished_at?: string | null } | null;
    local_datasets: { layer: string; exists: boolean; rows: number; bytes: number; files: string[] }[];
  };
  fabric: { configured: boolean; state: string; workspace: string | null; lakehouse: string | null; execution: string; message: string };
};
type CloudEvent = {
  event_id: number;
  operation: string;
  storage_mode: string;
  state: string;
  layer: string | null;
  object_name: string | null;
  bytes_transferred: number;
  message: string | null;
  started_at: string;
};
type StoredFile = { name: string; size: number; last_modified: string | null; etag: string | null };

export default function CloudIntegration() {
  const [status, setStatus] = useState<CloudStatus | null>(null);
  const [events, setEvents] = useState<CloudEvent[]>([]);
  const [files, setFiles] = useState<Record<string, StoredFile[]>>({});
  const [result, setResult] = useState('');
  const [error, setError] = useState('');
  const [busy, setBusy] = useState('');

  async function refresh() {
    const [statusResponse, historyResponse] = await Promise.all([
      fetch(`${API}/cloud/status`),
      fetch(`${API}/cloud/history?limit=30`),
    ]);
    const statusBody = await statusResponse.json();
    const historyBody = await historyResponse.json();
    if (!statusResponse.ok) throw new Error(statusBody.detail || `Cloud status failed: ${statusResponse.status}`);
    if (!historyResponse.ok) throw new Error(historyBody.detail || `Cloud history failed: ${historyResponse.status}`);
    setStatus(statusBody as CloudStatus);
    setEvents(historyBody.events as CloudEvent[]);
  }

  useEffect(() => {
    let active = true;
    Promise.all([
      fetch(`${API}/cloud/status`).then(async (response) => {
        const body = await response.json();
        if (!response.ok) throw new Error(body.detail || `Cloud status failed: ${response.status}`);
        return body as CloudStatus;
      }),
      fetch(`${API}/cloud/history?limit=30`).then(async (response) => {
        const body = await response.json();
        if (!response.ok) throw new Error(body.detail || `Cloud history failed: ${response.status}`);
        return body.events as CloudEvent[];
      }),
    ])
      .then(([cloudStatus, history]) => {
        if (active) {
          setStatus(cloudStatus);
          setEvents(history);
        }
      })
      .catch((cause: unknown) => {
        if (active) setError(cause instanceof Error ? cause.message : 'Could not load cloud integration status');
      });
    return () => { active = false; };
  }, []);

  async function runAction(action: 'test' | 'sync') {
    setBusy(action);
    setError('');
    setResult('');
    try {
      const response = await fetch(`${API}/cloud/${action === 'test' ? 'storage/test' : 'sync'}`, { method: 'POST' });
      const body = await response.json();
      if (!response.ok) throw new Error(body.detail || `Cloud ${action} failed: ${response.status}`);
      setResult(body.message || JSON.stringify(body));
      await refresh();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : `Cloud ${action} failed`);
      await refresh().catch(() => undefined);
    } finally {
      setBusy('');
    }
  }

  async function loadLayer(layer: string) {
    setBusy(`files-${layer}`);
    setError('');
    try {
      const response = await fetch(`${API}/cloud/storage/files/${layer}`);
      const body = await response.json();
      if (!response.ok) throw new Error(body.detail || `Could not list ${layer} files`);
      setFiles((previous) => ({ ...previous, [layer]: body.files as StoredFile[] }));
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : `Could not list ${layer} files`);
    } finally {
      setBusy('');
    }
  }

  const storage = status?.storage;
  return (
    <div className="shell">
      <aside>
        <div className="brand"><span>◆</span> DataFlow<span className="accent">360</span></div>
        {names.map((name, index) => <Link className={`nav${index === 9 ? ' selected' : ''}`} href={paths[index]} key={name}>{name}</Link>)}
      </aside>
      <main>
        <header>
          <div>
            <small>WORKSPACE / CLOUD INTEGRATION</small>
            <h1>Cloud storage and Fabric</h1>
            <p>Optional Azure Data Lake Storage Gen2 and Microsoft Fabric connections</p>
          </div>
          <div className="filters">
            <button disabled={!!busy} onClick={() => void runAction('test')}>{busy === 'test' ? 'Testing…' : 'Test configured storage'}</button>
            <button className="primary" disabled={!!busy} onClick={() => void runAction('sync')}>{busy === 'sync' ? 'Synchronizing…' : 'Sync Bronze / Silver / Gold'}</button>
          </div>
        </header>
        {error && <div className="error" role="alert">{error}</div>}
        {result && <article role="status"><p>{result}</p></article>}
        {!status && !error && <p>Loading cloud configuration from the backend…</p>}
        {status && storage && <>
          <section className="metrics">
            <article><small>ACTIVE EXECUTION MODE</small><h2>{storage.mode.toUpperCase()}</h2><p>{storage.mode === 'local' ? 'No Azure service is contacted in local mode.' : 'Azure mode is configured; connection is unverified until tested.'}</p></article>
            <article><small>STORAGE CONFIGURATION</small><h2>{storage.configured ? 'Configured' : 'Not configured'}</h2><p>{storage.account_host || storage.configuration_error || 'Local storage adapter'}</p></article>
            <article><small>CONNECTION TEST</small><h2>{storage.connection.state}</h2><p>{storage.connection.message}</p></article>
            <article><small>LAST SYNC</small><h2>{storage.last_sync?.state || 'Not run'}</h2><p>{storage.last_sync?.message || 'No synchronization has been recorded'}</p></article>
          </section>
          {storage.configuration_error && <div className="error" role="alert">{storage.configuration_error}</div>}
          <article>
            <h3>Execution modes and ADLS Gen2 file systems</h3>
            <p>Local adapter: {status.execution_modes.find((item) => item.mode === 'local')?.available ? 'available' : 'unavailable'}.
              Azure adapter: {status.execution_modes.find((item) => item.mode === 'azure')?.available ? 'SDK available; authentication and access still require a test' : 'not available in this runtime'}.
              Authentication: {storage.authentication}.</p>
            <p>Azure account: {storage.account_host || 'not configured'} · File systems are not created automatically.</p>
            <table><thead><tr><th>Layer</th><th>Configured file system</th><th>Local Parquet snapshot</th><th>Local size</th><th>Storage listing</th></tr></thead>
              <tbody>{layers.map((layer) => {
                const dataset = storage.local_datasets.find((item) => item.layer === layer);
                return <tr key={layer}>
                  <td>{layer}</td><td>{storage.containers[layer] || '—'}</td>
                  <td>{dataset?.exists ? dataset.files.join(', ') : 'Not generated'}</td>
                  <td>{dataset?.exists ? `${dataset.bytes.toLocaleString()} bytes` : '—'}</td>
                  <td><button disabled={!!busy} onClick={() => void loadLayer(layer)}>{busy === `files-${layer}` ? 'Loading…' : 'List stored files'}</button></td>
                </tr>;
              })}</tbody>
            </table>
          </article>
          <article>
            <h3>Stored objects</h3>
            {layers.map((layer) => <section key={layer}>
              <h4>{layer}</h4>
              {!files[layer] && <p>Use “List stored files” to query the configured adapter.</p>}
              {files[layer] && files[layer].length === 0 && <p>No stored files were returned by the adapter.</p>}
              {files[layer]?.length ? <table><thead><tr><th>Object</th><th>Bytes</th><th>Last modified</th><th>Action</th></tr></thead>
                <tbody>{files[layer].map((file) => <tr key={file.name}><td>{file.name}</td><td>{file.size.toLocaleString()}</td><td>{file.last_modified || '—'}</td>
                  <td><a href={`${API}/cloud/storage/files/${layer}/${file.name.split('/').map(encodeURIComponent).join('/')}`}>Download</a></td></tr>)}</tbody></table> : null}
            </section>)}
          </article>
          <article>
            <h3>Microsoft Fabric</h3>
            <p>State: {status.fabric.state} · Execution: {status.fabric.execution}</p>
            <p>{status.fabric.message}</p>
            <p>Workspace: {status.fabric.workspace || 'not configured'} · Lakehouse: {status.fabric.lakehouse || 'not configured'}</p>
            <p>Configuration alone does not connect to Fabric or prove notebook execution. Follow the Fabric guide to import and run the notebook in a real workspace.</p>
          </article>
          <article>
            <h3>Cloud operation history</h3>
            {events.length === 0 ? <p>No cloud or local storage operations have been recorded.</p> :
              <table><thead><tr><th>Time</th><th>Operation</th><th>Mode</th><th>State</th><th>Layer / object</th><th>Bytes</th><th>Message</th></tr></thead>
                <tbody>{events.map((event) => <tr key={event.event_id}><td>{new Date(event.started_at).toLocaleString()}</td><td>{event.operation}</td>
                  <td>{event.storage_mode}</td><td>{event.state}</td><td>{[event.layer, event.object_name].filter(Boolean).join(' / ') || '—'}</td>
                  <td>{event.bytes_transferred.toLocaleString()}</td><td>{event.message || '—'}</td></tr>)}</tbody></table>}
          </article>
        </>}
      </main>
    </div>
  );
}
