'use client';
import {useEffect,useState} from 'react';
import Link from 'next/link';
const API=process.env.NEXT_PUBLIC_API_URL||'http://localhost:8000';
export default function Quality(){
 const [data,setData]=useState<any>(null);
 useEffect(()=>{fetch(`${API}/quality`).then(r=>r.json()).then(setData)},[]);
 const names=['Overview','Sources','Data Quality','Medallion','Star Schema','Analytics','Inventory','Airflow'];
 const paths=['/','/sources','/quality','/medallion','/star-schema','/analytics','/inventory','/airflow'];
 return <div className="shell"><aside><div className="brand"><span>◆</span> DataFlow<span className="accent">360</span></div>{names.map((n,i)=><Link className={'nav'+(i===2?' selected':'')} href={paths[i]} key={n}>{n}</Link>)}</aside><main><header><div><small>WORKSPACE / DATA QUALITY</small><h1>Validation results</h1><p>Rejected records and quality rules</p></div></header>{data&&<><section className="metrics"><article><small>BRONZE ROWS</small><h2>{data.bronze}</h2></article><article><small>SILVER ROWS</small><h2>{data.silver}</h2></article></section><article><h3>Rejected reasons</h3><table><thead><tr><th>Reason</th><th>Rows</th></tr></thead><tbody>{data.reasons.map((r:any)=><tr key={r.reason}><td>{r.reason}</td><td>{r.rows}</td></tr>)}</tbody></table></article></>}</main></div>
}
