"use client";
import { useState } from "react";
export const number = (v,d=1) => v == null || !Number.isFinite(Number(v)) ? "—" : Number(v).toLocaleString("vi-VN",{maximumFractionDigits:d});
const colors=["#3977ff","#19b58e","#ef6670"];
const seasons=["2022-2023","2023-2024","2024-2025"];
const months=[8,9,10,11,12,1,2,3,4,5];
const monthNames=["Th8","Th9","Th10","Th11","Th12","Th1","Th2","Th3","Th4","Th5"];
const average=values=>{const valid=values.filter(v=>v!=null&&Number.isFinite(Number(v)));return valid.length?valid.reduce((s,v)=>s+Number(v),0)/valid.length:null;};
function Card({title,subtitle,children,className=""}) {return <article className={`a-card ${className}`}><div className="a-card-heading"><h2>{title}</h2>{subtitle&&<span>{subtitle}</span>}</div>{children}</article>;}
export function Stats({items}) {return <div className="a-stats">{items.map(([label,value,note,tone])=><article key={label}><span>{label}</span><strong className={tone||""}>{value}</strong>{note&&<small>{note}</small>}</article>)}</div>;}
export function Lines({series,labels=monthNames,unit="bàn"}) {
  const max=Math.max(1,...series.flatMap(s=>s.values.filter(v=>v!=null))),ceiling=Math.ceil(max/4)*4;
  const point=(i,v)=>[44+i*490/Math.max(1,labels.length-1),178-v/ceiling*144];
  return <div className="a-chart"><svg viewBox="0 0 570 222" role="img" aria-label={`${unit} theo ${labels.join(", ")}`}>
    {[0,1,2,3,4].map(n=><g key={n}><line x1="44" x2="540" y1={178-n*36} y2={178-n*36} stroke="#e9eff8"/><text x="33" y={182-n*36} textAnchor="end">{number(ceiling*n/4)}</text></g>)}
    {labels.map((label,i)=><text key={i} x={point(i,0)[0]} y="202" textAnchor="middle">{label}</text>)}
    {series.map((s,j)=><g key={s.name}>{s.values.map((v,i)=>v!=null&&i>0&&s.values[i-1]!=null?<line key={`l${i}`} x1={point(i-1,s.values[i-1])[0]} y1={point(i-1,s.values[i-1])[1]} x2={point(i,v)[0]} y2={point(i,v)[1]} stroke={colors[j%3]} strokeWidth="2.5"/>:null)}{s.values.map((v,i)=>v==null?null:<circle key={i} cx={point(i,v)[0]} cy={point(i,v)[1]} r="3.5" fill={colors[j%3]}><title>{s.name} · {labels[i]}: {number(v)} {unit}</title></circle>)}</g>)}
  </svg><div className="a-legend">{series.map((s,i)=><span key={s.name}><i style={{background:colors[i%3]}}/>{s.name}</span>)}</div></div>;
}
function Bars({rows,unit="",paired=false}) {
 const max=Math.max(1,...rows.flatMap(r=>[r.value,r.second].filter(v=>v!=null&&Number.isFinite(Number(v))).map(Number)));
 return <div className={`a-bars ${paired?"a-bars-paired":""}`}>{rows.map(r=><div className="a-bar-row" key={r.label}><span title={r.label}>{r.label}</span><div><div className="a-bar-track"><i style={{width:`${Math.max(0,Number(r.value)||0)/(r.max||max)*100}%`}}/></div>{paired&&<div className="a-bar-track a-bar-second"><i style={{width:`${Math.max(0,Number(r.second)||0)/(r.max||max)*100}%`}}/></div>}</div><strong>{number(r.value)}{unit}{paired&&<small>{number(r.second)}{unit}</small>}</strong></div>)}</div>;
}
function Outcome({matches}) {
 const values=[matches.filter(m=>m.home_goals>m.away_goals).length,matches.filter(m=>m.home_goals===m.away_goals).length,matches.filter(m=>m.home_goals<m.away_goals).length];
 return <div className="a-outcomes">{values.map((v,i)=><div key={i}><strong>{number(100*v/Math.max(1,matches.length))}%</strong><div className="a-outcome-bar" style={{height:`${Math.max(4,v/Math.max(...values,1)*132)}px`,background:colors[i]}}/><span>{["Chủ nhà thắng","Hòa","Đội khách thắng"][i]}</span><small>{number(v,0)} trận</small></div>)}</div>;
}
function Overview({data,season,Table}) {
 const matches=data.matches.filter(m=>m.season===season),metrics=data.match_results.season_metrics[season];
 const clubs=new Set(matches.flatMap(m=>[m.home_team,m.away_team]));
 const totalShots=average(matches.flatMap(m=>[m.home_stats.shots,m.away_stats.shots])),possession=average(matches.flatMap(m=>[m.home_stats.possession,m.away_stats.possession]));
 const series=seasons.map(s=>({name:s.replaceAll("20",""),values:months.map(month=>{const rows=data.matches.filter(m=>m.season===s&&Number(m.match_date.slice(5,7))===month);return rows.length?rows.reduce((sum,m)=>sum+m.home_goals+m.away_goals,0):null;})}));
 const perMatch=(key)=>average(matches.flatMap(m=>[m.home_stats[key],m.away_stats[key]]));
 const recent=[...matches].sort((a,b)=>b.match_date.localeCompare(a.match_date)).slice(0,8).map(m=>({date:m.match_date,fixture:`${m.home_name} – ${m.away_name}`,score:`${m.home_goals} – ${m.away_goals}`}));
 return <><Stats items={[["Đội tham dự",number(clubs.size,0),season],["Trận đấu",number(matches.length,0),"Lịch sử đã hoàn thành"],["Tổng bàn thắng",number(metrics.goals,0),"Bàn thắng được ghi nhận"],["Bàn / trận",number(metrics.goals_per_match,2),"Trung bình mùa giải"],["Sút / đội / trận",number(totalShots,2),"Thống kê hai đội"],["Kiểm soát bóng",`${number(possession)}%`,"Trung bình các trận"]]}/>
 <div className="a-overview-grid"><Card title="Xu hướng bàn thắng theo tháng" subtitle="Đối chiếu cả ba mùa"><Lines series={series}/></Card><Card title="Phân bố kết quả trận đấu" subtitle={season}><Outcome matches={matches}/></Card><Card title="Kết quả gần đây" subtitle={season}><Table rows={recent} columns={[["date","Ngày"],["fixture","Trận đấu"],["score","Tỉ số"]]}/></Card><Card title="Chỉ số nổi bật" subtitle="Trung bình mỗi đội / trận"><Bars rows={[["Dứt điểm","shots"],["Sút trúng đích","shots_on_target"],["Tạt bóng","crosses"],["Phạm lỗi","fouls_committed"],["Thẻ vàng","yellow_cards"],["Thẻ đỏ","red_cards"]].map(([label,key])=>({label,value:perMatch(key)}))}/></Card></div></>;
}
const rankFeatures=[
 {key:"ppg",label:"PPG",help:"Điểm trung bình mỗi trận",min:0,max:3,step:0.01},
 {key:"goal_difference_per_match",label:"Goal Difference/Game",help:"Hiệu số bàn thắng trung bình mỗi trận",min:-10,max:10,step:0.01},
 {key:"recent_form",label:"Recent Form",help:"Điểm trung bình trong 5 trận gần nhất",min:0,max:3,step:0.01},
 {key:"goals_scored_per_game",label:"Goals Scored/Game",help:"Số bàn thắng trung bình mỗi trận",min:0,max:10,step:0.01},
];
const emptyClub=(name="")=>({club_name:name,ppg:"",goal_difference_per_match:"",recent_form:"",goals_scored_per_game:""});
function minMaxScale(samples,keys){
 const ranges=Object.fromEntries(keys.map(key=>{const values=samples.map(row=>Number(row[key]));return [key,[Math.min(...values),Math.max(...values)]];}));
 return row=>keys.map(key=>{const value=Number(row[key]),[low,high]=ranges[key];return high===low?0:(value-low)/(high-low);});
}
function rankByKnn(input,samples){
 const keys=rankFeatures.map(feature=>feature.key),scale=minMaxScale(samples,keys),normalized=samples.map(row=>({...row,vector:scale(row)}));
 const scored=input.map((row,index)=>{const vector=scale(row),neighbors=normalized.map(sample=>({sample,distance:Math.sqrt(vector.reduce((sum,value,i)=>sum+(value-sample.vector[i])**2,0))})).sort((a,b)=>a.distance-b.distance||String(a.sample.season).localeCompare(String(b.sample.season))||String(a.sample.club_id).localeCompare(String(b.sample.club_id))).slice(0,5);return {...row,id:index,mean_neighbor_rank:neighbors.reduce((sum,item)=>sum+Number(item.sample.rank),0)/neighbors.length,nearest:neighbors};});
 return scored.sort((a,b)=>a.mean_neighbor_rank-b.mean_neighbor_rank||a.club_name.localeCompare(b.club_name,"vi")).map((row,index)=>({...row,predicted_rank:index+1}));
}
function Standings({data,Table}) {
 const samples=Array.isArray(data?.knn_samples)?data.knn_samples.map(row=>({...row,ppg:Number(row.points_per_mp),goal_difference_per_match:Number(row.goal_diff_per_match),goals_scored_per_game:Number(row.goals_for_per_match)})):[];
 const latest=[...new Set(samples.map(row=>row.season))].sort().at(-1);
 const defaults=samples.filter(row=>row.season===latest).sort((a,b)=>a.rank-b.rank).map(row=>Object.fromEntries(["club_name",...rankFeatures.map(feature=>feature.key)].map(key=>[key,row[key]])));
 const [clubs,setClubs]=useState(()=>defaults);
 const [ranked,setRanked]=useState([]),[validation,setValidation]=useState("");
 const update=(index,key,value)=>{setClubs(rows=>rows.map((row,i)=>i===index?{...row,[key]:value}:row));setRanked([]);};
 const calculate=()=>{
  const complete=clubs.every(row=>row.club_name.trim()&&rankFeatures.every(feature=>row[feature.key]!==""&&Number.isFinite(Number(row[feature.key]))));
  if(!complete){setValidation("Nhập tên đội và đủ bốn chỉ số cho tất cả các đội.");setRanked([]);return;}
  if(clubs.length<2){setValidation("Nhập chỉ số cho ít nhất hai đội để xếp hạng.");setRanked([]);return;}
  if(clubs.some(row=>rankFeatures.some(feature=>Number(row[feature.key])<feature.min||Number(row[feature.key])>feature.max))){setValidation("Giá trị nằm ngoài phạm vi cho phép: PPG/Recent Form 0–3, hiệu số −10–10, bàn thắng/trận 0–10.");setRanked([]);return;}
  if(new Set(clubs.map(row=>row.club_name.trim().toLocaleLowerCase("vi"))).size!==clubs.length){setValidation("Tên đội bóng không được trùng nhau.");setRanked([]);return;}
  if(samples.length<5){setValidation("Cần ít nhất 5 mẫu lịch sử để chạy KNN.");setRanked([]);return;}
  setValidation("");setRanked(rankByKnn(clubs.map(row=>Object.fromEntries(["club_name",...rankFeatures.map(feature=>feature.key)].map(key=>[key,key==="club_name"?row[key].trim():Number(row[key])]))),samples));
 };
 const resultRows=ranked.map(row=>({predicted_rank:row.predicted_rank,club_name:row.club_name,mean_neighbor_rank:Number(row.mean_neighbor_rank.toFixed(2)),nearest_reference:`${row.nearest[0]?.sample.club_name} · ${row.nearest[0]?.sample.season}`}));
 return <><Card title="Ước tính thứ hạng bằng KNN" subtitle={`${samples.length} mẫu CLB–mùa lịch sử · MinMax 0–1 · 5 láng giềng gần nhất`}>
  <p className="knn-intro">Đã điền chỉ số thực tế mùa {latest} làm dữ liệu nhập ban đầu, không phải dự báo mùa tiếp theo. Bạn có thể sửa bốn chỉ số hoặc thêm đội để thử kịch bản riêng. Tập tham chiếu gồm ba mùa 2022–2025.</p>
  <div className="knn-form-scroll"><table className="knn-input-table"><thead><tr><th>Đội bóng</th>{rankFeatures.map(feature=><th key={feature.key}><span>{feature.label}</span><small>{feature.help}</small></th>)}<th aria-label="Thao tác"/></tr></thead><tbody>{clubs.map((row,index)=><tr key={index}><td><input aria-label={`Tên đội ${index+1}`} value={row.club_name} onChange={event=>update(index,"club_name",event.target.value)} placeholder="Tên đội"/></td>{rankFeatures.map(feature=><td key={feature.key}><input aria-label={`${feature.label} · ${row.club_name||`đội ${index+1}`}`} type="number" min={feature.min} max={feature.max} step={feature.step} value={row[feature.key]} onChange={event=>update(index,feature.key,event.target.value)} placeholder="—"/></td>)}<td><button className="knn-remove" type="button" title={`Xóa ${row.club_name||"đội"}`} onClick={()=>{setClubs(rows=>rows.filter((_,i)=>i!==index));setRanked([]);}}>×</button></td></tr>)}</tbody></table></div>
  <div className="knn-actions"><button type="button" onClick={()=>{setClubs(rows=>[...rows,emptyClub()]);setRanked([]);}}>Thêm đội bóng</button><button type="button" onClick={calculate}>Tính thứ hạng KNN</button></div>{validation&&<p className="knn-validation" role="alert">{validation}</p>}
 </Card>
 {ranked.length>0&&<Card title="Kết quả ước tính" subtitle="Xếp theo thứ hạng trung bình của 5 láng giềng gần nhất"><Table rows={resultRows} columns={[["predicted_rank","Hạng ước tính KNN"],["club_name","Đội bóng"],["mean_neighbor_rank","Hạng trung bình của láng giềng"],["nearest_reference","Mẫu lịch sử gần nhất"]]} sortable/></Card>}
 <Card title="Cách tính" subtitle="KNN hồi cứu trên dữ liệu Silver đã công bố"><ol className="knn-steps"><li>Với từng chỉ số, lấy giá trị nhỏ nhất và lớn nhất trong các mùa tham chiếu.</li><li>Quy đổi mỗi giá trị về thang 0–1 theo công thức (giá trị − nhỏ nhất) / (lớn nhất − nhỏ nhất).</li><li>Tính khoảng cách Euclid giữa đội cần xếp hạng và các mẫu lịch sử; lấy 5 mẫu gần nhất.</li><li>Lấy trung bình hạng thật của 5 đội gần nhất, sau đó sắp xếp các đội người dùng nhập để tạo bảng xếp hạng.</li></ol><p className="a-note">Recent Form trong tập lịch sử được tính từ điểm trung bình của 5 trận cuối mùa: thắng 3 điểm, hòa 1 điểm, thua 0 điểm. Báo cáo metric KNN được tính hồi cứu theo mùa và chuẩn hóa MinMax chỉ trên các mùa dùng để huấn luyện.</p></Card></>;
}
export default function Insights({data,season,tab,Table}) {return tab==="home"?<Overview {...{data,season,Table}}/>:<Standings {...{data,Table}}/>;}
