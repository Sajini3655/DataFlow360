'use client';
import {useEffect,useState} from 'react';
import Link from 'next/link';
const API=process.env.NEXT_PUBLIC_API_URL||'http://localhost:8000';
const names=['Overview','Sources','Data Quality','Medallion','Star Schema','Analytics','Inventory','Processing','Airflow','Cloud Integration'];
const paths=['/','/sources','/quality','/medallion','/star-schema','/analytics','/inventory','/processing','/airflow','/cloud'];
export default function Analytics(){
 const [data,setData]=useState<any>(null),[category,setCategory]=useState(''),[region,setRegion]=useState('');
 const [startDate,setStartDate]=useState(''),[endDate,setEndDate]=useState('');
 const [error,setError]=useState('');
 async function refresh(){
  const query=new URLSearchParams();
  if(category)query.set('category',category);
  if(region)query.set('region',region);
  if(startDate)query.set('start_date',startDate);
  if(endDate)query.set('end_date',endDate);
  const response=await fetch(`${API}/analytics?${query}`);
  if(!response.ok)throw new Error(`Analytics request failed: ${response.status}`);
  setData(await response.json());
 }
 useEffect(()=>{refresh().catch(e=>setError(e.message))},[]);
 const money=(v:any)=>Number(v||0).toLocaleString('en-US',{style:'currency',currency:'USD'});
 const bar=(rows:any[],key:string)=>Math.max(1,...rows.map(x=>Number(x.revenue)));
 return <div className="shell"><aside><div className="brand"><span>◆</span> DataFlow<span className="accent">360</span></div>{names.map((n,i)=><Link className={'nav'+(i===5?' selected':'')} href={paths[i]} key={n}>{n}</Link>)}</aside><main><header><div><small>WORKSPACE / ANALYTICS</small><h1>Filtered sales analytics</h1><p>All metrics and charts are queried from Silver warehouse facts</p></div></header>{error&&<div className="error">{error}</div>}<article className="filters"><label>Category<select value={category} onChange={e=>setCategory(e.target.value)}><option value="">All categories</option>{data?.filters.categories.map((x:string)=><option key={x}>{x}</option>)}</select></label><label>Region<select value={region} onChange={e=>setRegion(e.target.value)}><option value="">All regions</option>{data?.filters.regions.map((x:string)=><option key={x}>{x}</option>)}</select></label><label>From<input type="date" value={startDate} onChange={e=>setStartDate(e.target.value)}/></label><label>To<input type="date" value={endDate} onChange={e=>setEndDate(e.target.value)}/></label><button className="primary" onClick={()=>refresh().catch(e=>setError(e.message))}>Apply filters</button></article>{data&&<><section className="metrics"><article><small>FILTERED REVENUE</small><h2>{money(data.metrics.revenue)}</h2></article><article><small>FILTERED UNITS</small><h2>{data.metrics.units}</h2></article><article><small>FILTERED LINES</small><h2>{data.metrics.records}</h2></article></section><article><h3>Revenue by month</h3>{data.monthly.map((m:any)=><div className="barrow" key={m.month}><span>{new Date(m.month).toLocaleDateString(undefined,{year:'numeric',month:'short'})}</span><div className="track"><div style={{width:`${Number(m.revenue)/bar(data.monthly,'revenue')*100}%`}}/></div><strong>{money(m.revenue)}</strong></div>)}</article><article><h3>Revenue by region</h3>{data.regions.map((r:any)=><div className="barrow" key={r.region}><span>{r.region}</span><div className="track"><div style={{width:`${Number(r.revenue)/bar(data.regions,'revenue')*100}%`}}/></div><strong>{money(r.revenue)}</strong></div>)}</article><article><h3>Revenue by category</h3><table><thead><tr><th>Category</th><th>Revenue</th></tr></thead><tbody>{data.categories.map((c:any)=><tr key={c.category}><td>{c.category}</td><td>{money(c.revenue)}</td></tr>)}</tbody></table></article></>}</main></div>
}
