'use client';
import { useEffect, useState } from 'react';
import Link from 'next/link';

const API = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';
const names = ['Overview', 'Sources', 'Data Quality', 'Medallion', 'Star Schema', 'Analytics', 'Inventory', 'Processing', 'Airflow', 'Cloud Integration'];
const paths = ['/', '/sources', '/quality', '/medallion', '/star-schema', '/analytics', '/inventory', '/processing', '/airflow', '/cloud'];

type LayerFile = {
  layer: string;
  exists: boolean;
  files: string[];
  rows: number;
  bytes: number;
  last_updated: number | null;
};
type Lineage = {
  lineage_id: number;
  pipeline_run_id: number | null;
  source_name: string;
  from_dataset: string;
  to_dataset: string;
  from_layer: string;
  to_layer: string;
  rows_written: number;
  created_at: string;
};
type MedallionData = {
  database: { bronze: number; silver: number; gold: number };
  parquet: LayerFile[];
  lineage: Lineage[];
};

export default function Medallion() {
  const [data, setData] = useState<MedallionData | null>(null);
  const [error, setError] = useState('');
  useEffect(() => {
    let active = true;
    fetch(`${API}/medallion`)
      .then(async (response) => {
        const body = await response.json();
        if (!response.ok) throw new Error(body.detail || `Medallion request failed: ${response.status}`);
        return body as MedallionData;
      })
      .then((body) => { if (active) setData(body); })
      .catch((e: unknown) => { if (active) setError(e instanceof Error ? e.message : 'Could not load medallion details'); });
    return () => { active = false; };
  }, []);

  return (
    <div className="shell">
      <aside><div className="brand"><span>◆</span> DataFlow<span className="accent">360</span></div>
        {names.map((name, index) => <Link className={`nav${index === 3 ? ' selected' : ''}`} href={paths[index]} key={name}>{name}</Link>)}
      </aside>
      <main>
        <header><div><small>WORKSPACE / MEDALLION</small><h1>Bronze, Silver &amp; Gold</h1>
          <p>Persisted PostgreSQL layers, Parquet outputs, and run lineage</p></div></header>
        {error && <div className="error" role="alert">{error}</div>}
        {!data && !error && <p>Loading layer and lineage data…</p>}
        {data && <>
          <section className="metrics">
            <article><small>BRONZE / POSTGRESQL</small><h2>{data.database.bronze.toLocaleString()}</h2><p>bronze_sales</p></article>
            <article><small>SILVER / POSTGRESQL</small><h2>{data.database.silver.toLocaleString()}</h2><p>silver_sales</p></article>
            <article><small>GOLD / POSTGRESQL</small><h2>{data.database.gold.toLocaleString()}</h2><p>gold_sales_fact</p></article>
            <article><small>LINEAGE EVENTS</small><h2>{data.lineage.length.toLocaleString()}</h2><p>Most recent 100 records</p></article>
          </section>
          <article><h3>Parquet datasets on disk</h3>
            <table><thead><tr><th>Layer</th><th>Status</th><th>Rows</th><th>Size</th><th>Last updated</th><th>Files</th></tr></thead>
              <tbody>{data.parquet.map((layer) => <tr key={layer.layer}>
                <td>{layer.layer}</td><td>{layer.exists ? 'persisted' : 'not generated'}</td>
                <td>{layer.rows.toLocaleString()}</td><td>{(layer.bytes / (1024 * 1024)).toFixed(2)} MiB</td>
                <td>{layer.last_updated ? new Date(layer.last_updated * 1000).toLocaleString() : '—'}</td>
                <td>{layer.files.length ? layer.files.join(', ') : '—'}</td>
              </tr>)}</tbody>
            </table>
          </article>
          <article><h3>Dataset lineage</h3>
            {data.lineage.length === 0 ? <p>No lineage has been recorded.</p> : <table>
              <thead><tr><th>Run</th><th>Source</th><th>From</th><th>To</th><th>Rows</th><th>Created</th></tr></thead>
              <tbody>{data.lineage.map((item) => <tr key={item.lineage_id}>
                <td>{item.pipeline_run_id ?? '—'}</td><td>{item.source_name}</td>
                <td>{item.from_dataset} ({item.from_layer})</td><td>{item.to_dataset} ({item.to_layer})</td>
                <td>{item.rows_written.toLocaleString()}</td><td>{new Date(item.created_at).toLocaleString()}</td>
              </tr>)}</tbody>
            </table>}
          </article>
        </>}
      </main>
    </div>
  );
}
