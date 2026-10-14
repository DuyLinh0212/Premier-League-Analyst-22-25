"use client";
import { useState } from "react";

const names=["Khởi đầu","Giữa lượt đi","Đầu lượt về","Nước rút"];
const ranges=["1–10","11–19","20–29","30–38"];
const colors=["#2563eb","#059669","#d97706"];
const metrics={ppg:"Điểm/trận",win_rate:"Tỷ lệ thắng (%)",gf_per_match:"Bàn thắng/trận",ga_per_match:"Bàn thua/trận"};
const fmt=n=>Number(n).toLocaleString("vi-VN",{maximumFractionDigits:2});

function LineChart({series,rolling=false,metric="ppg"}){
  const max=metric==="ppg"?3:metric==="win_rate"?100:Math.max(1,Math.ceil(Math.max(...series.flatMap(s=>s.values.map(v=>v.y)))));
  const x=n=>78+(n-(rolling?5:1))*718/(rolling?33:3),y=n=>320-n/max*264;
  return <><svg className="pl-phase-chart" viewBox="0 0 860 390" role="img" aria-label={rolling?"Phong độ 5 trận gần nhất":`Phong độ 4 giai đoạn · ${metrics[metric]}`}>
    <text x="78" y="23" fill="#334155" fontSize="13">{rolling?"Điểm/trận (5 trận gần nhất)":metrics[metric]}</text>
    {[0,1,2,3,4].map(i=><g key={`grid-${i}`}><line x1="78" x2="796" y1={y(max*i/4)} y2={y(max*i/4)} stroke="#dbe3ec" strokeDasharray="3 5"/><text x="64" y={y(max*i/4)+5} textAnchor="end" fontSize="12" fill="#475569">{fmt(max*i/4)}</text></g>)}
    {rolling?[10.5,19.5,29.5].map(n=><line key={`roll-line-${n}`} x1={x(n)} x2={x(n)} y1="56" y2="320" stroke="#94a3b8" strokeDasharray="5 5"/>):names.map((name,i)=><g key={`name-${name}`}><text x={x(i+1)} y="348" textAnchor="middle" fontSize="12" fill="#334155">{name}</text><text x={x(i+1)} y="369" textAnchor="middle" fontSize="11" fill="#64748b">Trận {ranges[i]}</text></g>)}
    {rolling&&[5,10,15,20,25,30,35,38].map(n=><text key={`roll-txt-${n}`} x={x(n)} y="348" textAnchor="middle" fontSize="12" fill="#475569">{n}</text>)}
    {series.map(s=><g key={`series-${s.name}`}><polyline points={s.values.map(v=>`${x(v.x)},${y(v.y)}`).join(" ")} fill="none" stroke={s.color} strokeWidth="3" strokeLinejoin="round"/>{s.values.map((v,i)=><circle key={`circle-${s.name}-${i}`} cx={x(v.x)} cy={y(v.y)} r={rolling?3.5:6} fill={s.color} stroke="white" strokeWidth="2"><title>{s.name} · {v.tip} · {fmt(v.y)}</title></circle>)}</g>)}
    {rolling&&<text x="437" y="378" textAnchor="middle" fontSize="12" fill="#64748b">Thứ tự trận trong mùa (không phải vòng đấu gốc)</text>}
  </svg><div className="pl-phase-legend">{series.map(s=><span key={`legend-${s.name}`}><i style={{borderColor:s.color}}/>{s.name}</span>)}</div></>;
}

// Display-only adaptation of club_phases.js in football_analysis_share_v3.
function momentum(report){let total=0;return [{index:0,value:0},...report.stages.flatMap(s=>s.fixtures).slice().sort((a,b)=>a.index-b.index).map(m=>({...m,value:total+=m.points===3?1:m.points===1?0:-1}))];}
function Momentum({primary,secondary,compare}){
  const a=momentum(primary),b=secondary?momentum(secondary):null;
  if(compare&&(!b||a.length!==b.length))return <p>Không đủ dữ liệu cùng mùa để đối chiếu.</p>;
  const values=compare?[...a,...b]:a;
  const limit=Math.ceil(Math.max(2,...values.map(v=>Math.abs(v.value)))/4)*4;
  const x=n=>64+n*904/38,y=v=>36+(limit-v)*299/(limit*2);
  const teams=compare?[{report:primary,values:a},{report:secondary,values:b}]:[{report:primary,values:a}];
  return <section className="pl-momentum"><div className="pl-momentum-heading"><h2>{compare?"Đối chiếu phong độ":"Phong độ cộng dồn"} · {primary.season}</h2><strong>{compare?`A: ${a.at(-1).value} / B: ${b.at(-1).value}`:`${a.at(-1).value>0?"+":""}${a.at(-1).value}`} · cuối mùa</strong></div><p>{primary.club_name}{compare?` (xanh) / ${secondary.club_name} (đỏ)`:" · Thắng +1 / Hòa 0 / Thua −1"}</p>
    <svg viewBox={`0 0 1000 ${compare?500:458}`} role="img" aria-label={compare?"Hai đường phong độ cộng dồn của đội A và đội B":"Phong độ cộng dồn thắng hòa thua"}>
      <text x="64" y="18" fill="#9db5c1" fontSize="12">{"Chỉ số phong độ (không phải điểm giải đấu)"}</text>
      {[-4,-3,-2,-1,0,1,2,3,4].map(i=><g key={`mom-grid-${i}`}><line x1="64" x2="968" y1={y(i*limit/4)} y2={y(i*limit/4)} stroke={i?"#29414c":"#bdad7a"}/><text x="50" y={y(i*limit/4)+4} textAnchor="end" fill="#97aeb9" fontSize="12">{i>0?"+":""}{i*limit/4}</text></g>)}
      {names.map((name,i)=><g key={`mom-name-${name}`}><text x={x([5.5,15,24.5,34][i])} y="359" textAnchor="middle" fontSize="12" fill="#b4c7d1">{name}</text><text x={x([5.5,15,24.5,34][i])} y="378" textAnchor="middle" fontSize="11" fill="#7795a6">Trận {ranges[i]}</text>{i<3&&<line x1={x([10.5,19.5,29.5][i])} x2={x([10.5,19.5,29.5][i])} y1="36" y2="335" stroke="#29414c" strokeDasharray="3 6"/>}</g>)}
      {teams.map((team,k)=>{const color=k?"#ec365c":"#60a8d2",line=team.values.map((v,i)=>`${i?"L":"M"}${x(v.index)},${y(v.value)}`).join(" ");return <g key={`line-${k}`}><path d={line} fill="none" stroke={color} strokeWidth="3" strokeLinejoin="round"/>{team.values.map((v,idx)=><circle key={`circle-${team.report?.club_id||k}-${v.index||idx}`} cx={x(v.index)} cy={y(v.value)} r="4.3" fill={color} stroke="#061620"><title>{team.report.club_name} · Trận {v.index} · {v.date||"Bắt đầu mùa"} · {v.opponent||""} · Chỉ số {v.value}</title></circle>)}</g>;})}
      {teams.map((team,k)=><g key={`rect-${team.report?.club_id||k}-${k}`}><rect x="64" y={399+k*44} width="904" height="39" fill={k?"#ec365c":"#60a8d2"} opacity=".09"/><text x="72" y={413+k*44} fill={k?"#ec365c":"#60a8d2"} fontSize="11">{team.report.club_name} · thắng ● / hòa ◇ / thua ×</text>{team.values.slice(1).map((v,idx)=><text key={`txt-${team.report?.club_id||k}-${v.index||idx}`} x={x(v.index)} y={432+k*44} textAnchor="middle" fill={k?"#ec365c":"#60a8d2"} fontSize="13">{v.points===3?"●":v.points===1?"◇":"×"}<title>Trận {v.index} · {v.date} · {v.opponent} · {v.goals_for}–{v.goals_against}</title></text>)}</g>)}
    </svg><p className="pl-momentum-note">{compare?"Mỗi CLB có một đường riêng: A xanh, B đỏ. Thắng +1, hòa 0, thua −1; cộng dồn từ đầu mùa. So cùng thứ tự trận, không phải cùng ngày.":"Đường bắt đầu từ 0, thắng lên 1, thua xuống 1, hòa giữ nguyên."} Đây là chỉ số mô tả, không phải điểm bảng xếp hạng.</p>
  </section>;
}

export default function PhaseCharts({data,season,club,compare}){
  const [view,setView]=useState("history"),[metric,setMetric]=useState("ppg");
  const reports=Object.values(data.seasons||{}).flat().filter(r=>r.club_id===club);
  const primary=(data.seasons?.[season]||[]).find(r=>r.club_id===club),secondary=(data.seasons?.[season]||[]).find(r=>r.club_id===compare);
  const comparing=view==="compare";
  const selected=comparing?[primary,secondary].filter(Boolean):reports;
  const series=selected.map((r,i)=>({name:comparing?r.club_name:r.season,color:comparing?(i?"#ec365c":"#2563eb"):colors[i%colors.length],values:r.stages.map((s,j)=>({x:j+1,y:s[metric],tip:s.name}))}));
  if(!primary)return <p>CLB không có dữ liệu mùa đã chọn.</p>;
  return <div className="pl-phase-charts"><div className="u-controls"><label>Góc nhìn biểu đồ<select value={view} onChange={e=>setView(e.target.value)}><option value="history">Một CLB qua các mùa</option><option value="compare">So sánh hai CLB trong mùa chọn</option></select></label><label>Chỉ số biểu đồ<select value={metric} onChange={e=>setMetric(e.target.value)}>{Object.entries(metrics).map(([key,label])=><option key={key} value={key}>{label}</option>)}</select></label></div>
    <section className="pl-phase-card"><h2>Phong độ 4 giai đoạn · {primary.club_name}</h2><p>{comparing?`Mùa ${season}.` :"Mỗi đường là một mùa có dữ liệu; không điền 0 cho mùa ngoài Premier League."}{metric==="ga_per_match"&&" Bàn thua/trận: thấp hơn tốt hơn."}</p>{comparing&&!secondary?<p>CLB đối chiếu không có mùa này.</p>:<LineChart series={series} metric={metric}/>}</section>
    <Momentum primary={primary} secondary={secondary} compare={comparing}/>
  </div>;
}
