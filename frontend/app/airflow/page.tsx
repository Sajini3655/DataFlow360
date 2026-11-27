'use client';
import {useEffect,useState} from 'react';
import Link from 'next/link';
const API=process.env.NEXT_PUBLIC_API_URL||'http://localhost:8000';
const names=['Overview','Sources','Data Quality','Medallion','Star Schema','Analytics','Inventory','Airflow'];
const paths=['/','/sources','/quality','/medallion','/star-schema','/analytics','/inventory','/airflow'];
type Run={job_id:string;dag_run_id:string;state:string;source:string};

export default function Airflow(){
 const [busy,setBusy]=useState(false),[run,setRun]=useState<Run|null>(null),[status,setStatus]=useState<any>(null),[error,setError]=useState('');
 useEffect(()=>{
  if(!run?.dag_run_id)return;
  let active=true;
  const poll=async()=>{
   try{
    const response=await fetch(`${API}/pipeline/runs/${encodeURIComponent(run.dag_run_id)}`);
    if(!response.ok)throw new Error(`Status request failed: ${response.status}`);
    const value=await response.json();
    if(active)setStatus(value);
   }catch(e:any){if(active)setError(e.message)}
  };
  void poll();
  const timer=window.setInterval(()=>void poll(),3000);
  return ()=>{active=false;window.clearInterval(timer)};
 },[run?.dag_run_id]);

 async function start(){
  setBusy(true);setError('');setStatus(null);
  try{
   const response=await fetch(`${API}/pipeline/run?incremental=true&source=csv`,{method:'POST'});
   const body=await response.json();
   if(!response.ok)throw new Error(body.detail||`Trigger failed: ${response.status}`);
   setRun(body);
  }catch(e:any){setError(e.message)}finally{setBusy(false)}
 }
 const state=status?.state||run?.state||'Ready';
 return <div className="shell"><aside><div className="brand"><span>◆</span> DataFlow<span className="accent">360</span></div>{names.map((n,i)=><Link className={'nav'+(i===7?' selected':'')} href={paths[i]} key={n}>{n}</Link>)}</aside><main><header><div><small>WORKSPACE / AIRFLOW</small><h1>Pipeline orchestration</h1><p>Trigger and monitor the actual Airflow DAG execution</p></div><button className="primary" disabled={busy} onClick={start}>{busy?'Submitting…':'▶ Trigger pipeline'}</button></header>{error&&<div className="error">{error}</div>}<section className="metrics"><article><small>RUN STATE</small><h2>{state}</h2></article><article><small>DAG RUN ID</small><h2 className="compact">{run?.dag_run_id||'—'}</h2></article><article><small>TRIGGER SOURCE</small><h2>{run?'Apache Airflow':'—'}</h2></article></section>{status&&<article><h3>Airflow task progress</h3><table><thead><tr><th>Task</th><th>State</th></tr></thead><tbody>{status.tasks.map((task:any)=><tr key={task.task_id}><td>{task.task_id}</td><td>{task.state||'queued'}</td></tr>)}</tbody></table><p>Started: {status.start_date||'Not started'} · Finished: {status.end_date||'In progress'}</p></article>}<article><h3>Airflow web UI</h3><p>Container health and REST API status are checked separately from pipeline state.</p><a href="http://localhost:8080" target="_blank" rel="noreferrer">Open Airflow UI</a></article></main></div>
}
