'use client';
import {useEffect,useState} from 'react';
import Link from 'next/link';
const API=process.env.NEXT_PUBLIC_API_URL||'http://localhost:8000';
export default function Inventory(){
 const [data,setData]=useState<any>(null);
 useEffect(()=>{fetch(`${API}/inventory`).then(r=>r.json()).then(setData)},[]);
 const names=['Overview','Sources','Data Quality','Medallion','Star Schema','Analytics','Inventory','Processing','Airflow','Cloud Integration'];
 const paths=['/','/sources','/quality','/medallion','/star-schema','/analytics','/inventory','/processing','/airflow','/cloud'];
 return <div className="shell"><aside><div className="brand"><span>◆</span> DataFlow<span className="accent">360</span></div>{names.map((n,i)=><Link className={'nav'+(i===6?' selected':'')} href={paths[i]} key={n}>{n}</Link>)}</aside><main><header><div><small>WORKSPACE / INVENTORY</small><h1>Inventory performance</h1><p>Product sales and revenue</p></div></header>{data&&<article><table><thead><tr><th>Product</th><th>Category</th><th>Units sold</th><th>Revenue</th></tr></thead><tbody>{data.products.map((p:any)=><tr key={p.product_name}><td>{p.product_name}</td><td>{p.category}</td><td>{p.quantity_sold}</td><td>${Number(p.revenue).toLocaleString()}</td></tr>)}</tbody></table></article>}</main></div>
}
