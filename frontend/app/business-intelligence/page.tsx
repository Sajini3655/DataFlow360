'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import Link from 'next/link';

const API = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';
const names = ['Overview', 'Sources', 'Data Quality', 'Medallion', 'Star Schema', 'Analytics', 'Inventory', 'Processing', 'Airflow', 'Cloud Integration', 'Business Intelligence'];
const paths = ['/', '/sources', '/quality', '/medallion', '/star-schema', '/analytics', '/inventory', '/processing', '/airflow', '/cloud', '/business-intelligence'];

type Dataset = {
  name: string;
  schema: string;
  relation: string;
  description: string;
  kind: 'warehouse' | 'dbt';
  row_count: number;
  last_refreshed: string | null;
  revenue_total: string | null;
};

type Model = {
  primary_source: string;
  grain: Record<string, string>;
  relationships: { many: string; many_key: string; one: string; one_key: string }[];
  filter_direction: string;
  dbt_models: string[];
};

export default function BusinessIntelligence() {
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [model, setModel] = useState<Model | null>(null);
  const [kind, setKind] = useState<'all' | 'warehouse' | 'dbt'>('all');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  const refresh = useCallback(async () => {
    setLoading(true);
    setError('');
    try {
      const [datasetsResponse, modelResponse] = await Promise.all([
        fetch(`${API}/bi/datasets`),
        fetch(`${API}/bi/model`),
      ]);
      const [datasetsBody, modelBody] = await Promise.all([
        datasetsResponse.json(),
        modelResponse.json(),
      ]);
      if (!datasetsResponse.ok) {
        throw new Error(datasetsBody.detail || `Dataset request failed: ${datasetsResponse.status}`);
      }
      if (!modelResponse.ok) {
        throw new Error(modelBody.detail || `Semantic model request failed: ${modelResponse.status}`);
      }
      setDatasets(datasetsBody.datasets as Dataset[]);
      setModel(modelBody as Model);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'Could not load business intelligence data');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { void refresh(); }, [refresh]);

  const visibleDatasets = useMemo(
    () => kind === 'all' ? datasets : datasets.filter((dataset) => dataset.kind === kind),
    [datasets, kind],
  );
  const warehouseCount = datasets.filter((dataset) => dataset.kind === 'warehouse').length;
  const dbtCount = datasets.filter((dataset) => dataset.kind === 'dbt').length;
  const sales = datasets.find((dataset) => dataset.name === 'fact_sales');
  const latestRefresh = datasets
    .map((dataset) => dataset.last_refreshed)
    .filter((timestamp): timestamp is string => timestamp !== null)
    .sort()
    .at(-1);
  const money = (value: string | null) => value === null
    ? '—'
    : Number(value).toLocaleString('en-US', { style: 'currency', currency: 'USD' });

  return (
    <div className="shell">
      <aside>
        <div className="brand"><span>◆</span> DataFlow<span className="accent">360</span></div>
        {names.map((name, index) => (
          <Link className={`nav${index === 10 ? ' selected' : ''}`} href={paths[index]} key={name}>
            {name}
          </Link>
        ))}
      </aside>
      <main>
        <header>
          <div>
            <small>WORKSPACE / BUSINESS INTELLIGENCE</small>
            <h1>Business Intelligence</h1>
            <p>PostgreSQL reporting datasets and Power BI assembly resources</p>
          </div>
          <button className="primary" disabled={loading} onClick={() => void refresh()}>
            {loading ? 'Refreshing…' : 'Refresh datasets'}
          </button>
        </header>
        {error && <div className="error" role="alert">{error}</div>}
        {loading && <p>Loading reporting dataset metadata from the backend…</p>}
        {!loading && !error && <>
          <section className="metrics">
            <article><small>WAREHOUSE REPORTING VIEWS</small><h2>{warehouseCount}</h2><p>Counts queried from PostgreSQL</p></article>
            <article><small>AVAILABLE DBT MARTS</small><h2>{dbtCount}</h2><p>Optional models currently present in analytics</p></article>
            <article><small>GOLD SALES LINES</small><h2>{(sales?.row_count ?? 0).toLocaleString()}</h2><p>Unique order and line grain</p></article>
            <article><small>GOLD FACT REVENUE</small><h2>{money(sales?.revenue_total ?? null)}</h2><p>Sum of persisted Gold revenue</p></article>
          </section>

          <article>
            <h3>Available reporting datasets</h3>
            <p>Row counts and revenue are queried from the reporting views. Refresh timestamps are shown only when the source exposes one.</p>
            <div className="filters">
              <label>Source
                <select value={kind} onChange={(event) => setKind(event.target.value as typeof kind)}>
                  <option value="all">All datasets</option>
                  <option value="warehouse">PostgreSQL warehouse</option>
                  <option value="dbt">dbt analytics marts</option>
                </select>
              </label>
            </div>
            <table>
              <thead><tr><th>Dataset</th><th>Source</th><th>Rows</th><th>Revenue</th><th>Last updated</th><th>CSV export</th></tr></thead>
              <tbody>
                {visibleDatasets.map((dataset) => (
                  <tr key={dataset.name}>
                    <td><strong>{dataset.name}</strong><br /><small>{dataset.description}</small></td>
                    <td>{dataset.schema}.{dataset.relation}</td>
                    <td>{dataset.row_count.toLocaleString()}</td>
                    <td>{money(dataset.revenue_total)}</td>
                    <td>{dataset.last_refreshed ? new Date(dataset.last_refreshed).toLocaleString() : 'Not tracked by source'}</td>
                    <td><a href={`${API}/bi/exports/${encodeURIComponent(dataset.name)}.csv`}>Download CSV</a></td>
                  </tr>
                ))}
              </tbody>
            </table>
          </article>

          <section className="metrics">
            <article>
              <small>LATEST SOURCE REFRESH</small>
              <h2>{latestRefresh ? new Date(latestRefresh).toLocaleString() : 'Not available'}</h2>
              <p>Latest persisted timestamp exposed by a dataset</p>
            </article>
            <article>
              <small>POWER BI CONNECTIVITY</small>
              <h2>PostgreSQL + CSV</h2>
              <p>Use Import mode first; DirectQuery is connector-supported but not locally validated.</p>
            </article>
          </section>

          <article>
            <h3>Semantic model overview</h3>
            <p>{model?.primary_source}</p>
            <p>{model?.filter_direction}</p>
            <p>{model?.relationships.length ?? 0} recommended one-to-many relationships. See the semantic model guide for keys, grains, date hierarchy, and measures.</p>
            <table>
              <thead><tr><th>Fact table</th><th>Grain</th></tr></thead>
              <tbody>{Object.entries(model?.grain ?? {}).map(([name, grain]) => (
                <tr key={name}><td>{name}</td><td>{grain}</td></tr>
              ))}</tbody>
            </table>
          </article>

          <article>
            <h3>Connect Power BI Desktop</h3>
            <ol>
              <li>Choose Get data → PostgreSQL database; use <code>localhost:5432</code> and the database from the local <code>.env</code>.</li>
              <li>Load the <code>bi</code> schema views and create the documented single-direction relationships.</li>
              <li>Import DAX measures, set <code>Dim Date[full_date]</code> as the date table, and build the five report pages.</li>
              <li>Use the CSV downloads above as a file-based alternative. The CSVs are live exports, not automatically refreshed files.</li>
            </ol>
            <p>This Next.js dashboard shows real backend metadata; it is not an embedded Power BI report. No Power BI Desktop refresh or report rendering is claimed.</p>
            <p>See <code>docs/powerbi-guide.md</code>, <code>docs/power-query.md</code>, and <code>docs/semantic-model.md</code> in the repository for detailed setup.</p>
          </article>
        </>}
      </main>
    </div>
  );
}
