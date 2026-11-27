'use client';
import {useEffect,useState} from 'react';
import Link from 'next/link';
const API=process.env.NEXT_PUBLIC_API_URL||'http://localhost:8000';
export default function Medallion(){
 const [data,setData]=useState<any>(null);
 useEffect(()=>{fetch(`${API}/overview`).then(r=>r.json()).then(setData)},[]);
 const names=['Overview','Sources','Data Quality','Medallion','Star Schema','Analytics','Inventory','Airflow'];
 const paths=['/','/sources','/quality','/medallion','/star-schema','/analytics','/inventory','/airflow'];
 return <div className="shell"><aside><div className="brand"><span>◆</span> DataFlow<span className="accent">360</span></div>{names.map((n,i)=><Link className={'nav'+(i===3?' selected':'')} href={paths[i]} key={n}>{n}</Link>)}</aside><main><header><div><small>WORKSPACE / MEDALLION</small><h1>Bronze, Silver & Gold</h1><p>Persistent SQL and Parquet datasets</p></div></header>{data&&<section className="metrics"><article><small>BRONZE / SQL</small><h2>{data.layers.bronze}</h2><p>bronze_sales table</p></article><article><small>SILVER / SQL</small><h2>{data.layers.silver}</h2><p>silver_sales table</p></article><article><small>GOLD / SQL</small><h2>{data.layers.gold}</h2><p>gold_sales_fact table</p></article><article><small>PARQUET</small><h2>3 files</h2><p>/app/parquet</p></article></section>}</main></div>
}
