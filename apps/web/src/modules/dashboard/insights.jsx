"use client";
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
function Standings({data,season="2025-2026",Table}) {
 if(season!=="2025-2026") return <Card title={`Bảng xếp hạng thực tế · ${season.replace("-","/")}`} subtitle="Thứ hạng lấy từ dữ liệu Silver đã chuẩn hóa"><Table rows={data.standings||[]} columns={[["rank","Hạng thực tế"],["club_name","Đội bóng"],["matches_played","Trận"],["wins","Thắng"],["draws","Hòa"],["losses","Thua"],["goals_for","Bàn thắng"],["goals_against","Bàn thua"],["goal_diff","Hiệu số"],["points","Điểm"]]} sortable/></Card>;
 const predictions=(data.knn||[]).filter(r=>r.query_season===season).map(r=>({...r,actual_rank:null,actual_points:r.actual_points==null?null:Number(r.actual_points),predicted_rank:Number(r.predicted_rank),predicted_points:Number(r.predicted_points),top4_pct:100*Number(r.top4_probability)})).sort((a,b)=>a.predicted_rank-b.predicted_rank);
 return <><Card title="Dự đoán bảng xếp hạng bằng KNN · 2025/26" subtitle="Mùa mô phỏng chưa có thứ hạng thực tế; KNN ước tính thứ hạng dựa trên các mùa tham chiếu">{!predictions.length&&<p>Chưa có báo cáo KNN cho mùa 2025/26. Hãy chạy lại Airflow sau khi Kafka đã ghi mùa này lên HDFS.</p>}<Table rows={predictions} columns={[["query_club_name","Đội bóng"],["actual_rank","Hạng thực tế"],["predicted_rank","Hạng ước tính KNN"],["actual_points","Điểm bảng mô phỏng"],["predicted_points","Điểm ước tính KNN"],["top4_pct","Tỉ lệ láng giềng Top 4 (%)"]]} sortable/></Card><Card title="Điểm ước tính theo KNN" subtitle="5 láng giềng gần nhất từ các mùa tham chiếu"><Bars rows={predictions.slice(0,5).map(r=>({label:r.query_club_name,value:r.predicted_points}))}/><p className="a-note">2025/26 hiện được tạo bởi luồng Kafka mô phỏng. KNN dùng thống kê toàn mùa đã có để ước tính thứ hạng; đây chưa phải dự báo trước khi mùa giải diễn ra.</p></Card></>;
}
export default function Insights({data,season,tab,Table}) {return tab==="home"?<Overview {...{data,season,Table}}/>:<Standings {...{data,season,Table}}/>;}
