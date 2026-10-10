"use client";
import {useState} from "react";
const number=v=>v==null?"—":Number(v).toLocaleString("vi-VN",{maximumFractionDigits:2});
export default function Market({data,season,Table}) {
  const [first,setFirst]=useState("ARS"),[second,setSecond]=useState("CHE");
  const metrics=data.season_metrics[season],timing=data.goal_timing.filter(r=>r.season===season);
  const max=Math.max(1,...timing.map(r=>r.count));
  const a=data.club_profiles[first]?.seasons[season],b=data.club_profiles[second]?.seasons[season];
  const labels={points:"Điểm",rank:"Thứ hạng",goals_for:"Bàn thắng",goals_against:"Bàn thua",gross_spend_mil_eur:"Chi mua (triệu EUR)",sales_income_mil_eur:"Thu bán (triệu EUR)",net_spend_mil_eur:"Chi ròng (triệu EUR)",cost_per_point:"Triệu EUR / điểm",wage_mil_gbp:"Quỹ lương (triệu GBP)"};
  return <><div className="u-season-stats">{[["transfers","Giao dịch"],["spend_mil","Chi tiêu · triệu EUR"],["matches","Trận"],["goals","Bàn thắng"],["goals_per_match","Bàn / trận"]].map(([key,label])=><article key={key}><span>{label}</span><strong>{number(metrics[key])}</strong></article>)}</div>
    <h2>Bàn thắng theo khoảng 15 phút</h2><div className="u-timing">{timing.map(r=><article key={r.minute_bracket}><span>{r.minute_bracket}</span><div className="u-bar"><i style={{width:`${r.count/max*100}%`}}/></div><strong>{r.count} bàn</strong></article>)}</div>
    <h2>Đối chiếu CLB cùng mùa</h2><div className="u-controls">{[[first,setFirst,"CLB 1"],[second,setSecond,"CLB 2"]].map(([value,set,label])=><label key={label}>{label}<select value={value} onChange={e=>set(e.target.value)}>{data.dim_clubs.map(c=><option key={c.club_id} value={c.club_id}>{c.club_name}</option>)}</select></label>)}</div>
    <p>{data.club_profiles[first]?.club_name}: {a?.has_played?"có dữ liệu":"không tham dự Premier League mùa này"} · {data.club_profiles[second]?.club_name}: {b?.has_played?"có dữ liệu":"không tham dự Premier League mùa này"}</p>
    <Table rows={Object.entries(labels).map(([key,label])=>({metric:label,first:a?.has_played?a[key]:null,second:b?.has_played?b[key]:null}))} columns={[["metric","Chỉ số"],["first",data.club_profiles[first]?.club_name],["second",data.club_profiles[second]?.club_name]]}/>
    <details><summary>Bảng xếp hạng {season}</summary><Table rows={data.team_season_data.filter(r=>r.season===season)} columns={[["rank","Hạng"],["club_name","CLB"],["points","Điểm"],["wins","Thắng"],["draws","Hòa"],["losses","Thua"],["goals_for","Bàn thắng"],["goals_against","Bàn thua"]]}/></details>
    <details><summary>30 thương vụ lớn nhất trong ba mùa</summary><Table rows={data.top_transfers} columns={[["player_name","Cầu thủ"],["season","Mùa"],["from_club_name","CLB bán"],["to_club_name","CLB mua"],["fee_eur","Phí EUR"]]}/></details>
    <details><summary>Hồ sơ CLB qua các mùa tham dự</summary><Table rows={Object.values(data.club_profiles)} columns={[["club_name","CLB"],["seasons_played","Số mùa"],["avg_points","Điểm TB"],["total_spend","Chi mua · triệu EUR"],["total_income","Thu bán · triệu EUR"],["cost_per_point","Triệu EUR / điểm"]]}/></details></>;
}
