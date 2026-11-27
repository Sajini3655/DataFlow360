'use client';
import {useEffect,useState} from 'react';
import Link from 'next/link';
const API=process.env.NEXT_PUBLIC_API_URL||'http://localhost:8000';
export default function Star(){
 const [data,setData]=useState<any>(null);
 useEffect(()=>{fetch(`${API}/dimensions`).then(r=>r.json()).then(setData)},[]);
 const names=['Overview','Sources','Data Quality','Medallion','Star Schema','Analytics','Inventory','Processing','Airflow','Cloud Integration'];
 const paths=['/','/sources','/quality','/medallion','/star-schema','/analytics','/inventory','/processing','/airflow','/cloud'];
 return <div className="shell"><aside><div className="brand"><span>◆</span> DataFlow<span className="accent">360</span></div>{names.map((n,i)=><Link className={'nav'+(i===4?' selected':'')} href={paths[i]} key={n}>{n}</Link>)}</aside><main><header><div><small>WORKSPACE / STAR SCHEMA</small><h1>Dimensions and facts</h1><p>Customer, product, date, region, supplier</p></div></header>{data&&<section className="metrics">{[['Customers',data.customers],['Products',data.products],['Dates',data.dates],['Regions',data.regions],['Suppliers',data.suppliers],['Sales facts',data.sales_facts],['Inventory facts',data.inventory_facts]].map(([label,value])=><article key={String(label)}><small>{String(label).toUpperCase()}</small><h2>{value}</h2></article>)}</section>}</main></div>
}
