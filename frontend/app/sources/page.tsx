'use client';
import {useEffect,useState} from 'react';
import Link from 'next/link';
const API=process.env.NEXT_PUBLIC_API_URL||'http://localhost:8000';
export default function Sources(){
 const [data,setData]=useState<any>(null);
 const [result,setResult]=useState<any>(null),[busy,setBusy]=useState('');
 useEffect(()=>{fetch(`${API}/adapters`).then(r=>r.json()).then(setData)},[]);
 async function ingest(source:string){
  setBusy(source);setResult(null);
  try{
   const response=await fetch(`${API}/pipeline/run?source=${source}&incremental=true`,{method:'POST'});
   const body=await response.json();
   if(!response.ok)throw new Error(body.detail||`Request failed: ${response.status}`);
   setResult(body);
  }catch(e:any){setResult({error:e.message})}finally{setBusy('')}
 }
 const names=['Overview','Sources','Data Quality','Medallion','Star Schema','Analytics','Inventory','Airflow'];
 const paths=['/','/sources','/quality','/medallion','/star-schema','/analytics','/inventory','/airflow'];
 return <div className="shell"><aside><div className="brand"><span>◆</span> DataFlow<span className="accent">360</span></div>{names.map((n,i)=><Link className={'nav'+(i===1?' selected':'')} href={paths[i]} key={n}>{n}</Link>)}</aside><main><header><div><small>WORKSPACE / SOURCES</small><h1>Source adapters</h1><p>Queue actual Airflow runs for CSV, JSON, REST, or PostgreSQL</p></div></header>{data&&<section className="metrics">{Object.entries(data).map(([name,value]:any)=><article key={name}><small>{name.toUpperCase()} ADAPTER</small><h2>{value.status}</h2><p>{value.sample||value.source}</p><button className="primary" disabled={!!busy} onClick={()=>ingest(name)}>{busy===name?'Submitting…':`Run ${name}`}</button></article>)}</section>}{result&&<article><h3>Airflow submission result</h3><pre>{JSON.stringify(result,null,2)}</pre></article>}</main></div>
}
