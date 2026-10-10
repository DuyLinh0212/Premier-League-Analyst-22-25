"use client";
import {useState} from "react";
import {Card,Stats,Bars,number} from "./insights";
export default function Market({data,season,Table}) {
 const [first,setFirst]=useState("ARS"),[second,setSecond]=useState("CHE");
 const metrics=data.season_metrics[season],timing=data.goal_timing.filter(r=>r.season===season);
 const a=data.club_profiles[first]?.seasons[season],b=data.club_profiles[second]?.seasons[season];
 const labels={points:"Điểm",rank:"Thứ hạng",goals_for:"Bàn thắng",goals_against:"Bàn thua",gross_spend_mil_eur:"Chi mua (triệu EUR)",sales_income_mil_eur:"Thu bán (triệu EUR)",net_spend_mil_eur:"Chi ròng (triệu EUR)",cost_per_point:"Triệu EUR / điểm",wage_mil_gbp:"Quỹ lương (triệu GBP)"};
 return <><div className="u-season-stats"><Stats items={[["Giao dịch",number(metrics.transfers,0),season,"blue"],["Chi tiêu",`€${number(metrics.spend_mil/1000,2)}B`,"Phí đã được ghi nhận","blue"],["Trận đấu",number(metrics.matches,0),"Kết quả mùa giải"],["Bàn thắng",number(metrics.goals,0),"Dữ liệu sự kiện","green"],["Bàn / trận",number(metrics.goals_per_match,2),"Trung bình mùa giải"]]}/></div>
 <div className="a-two-col"><Card title="Bàn thắng theo khoảng 15 phút" subtitle={season}><Bars rows={timing.map(r=>({label:r.minute_bracket,value:r.count}))}/></Card>
 <Card title="Đối chiếu CLB cùng mùa"><div className="u-controls">{[[first,setFirst,"CLB 1"],[second,setSecond,"CLB 2"]].map(([value,set,label])=><label key={label}>{label}<select value={value} onChange={e=>set(e.target.value)}>{data.dim_clubs.map(c=><option key={c.club_id} value={c.club_id}>{c.club_name}</option>)}</select></label>)}</div><Table rows={Object.entries(labels).map(([key,label])=>({metric:label,first:a?.has_played?a[key]:null,second:b?.has_played?b[key]:null}))} columns={[["metric","Chỉ số"],["first",data.club_profiles[first]?.club_name],["second",data.club_profiles[second]?.club_name]]}/>{(!a?.has_played||!b?.has_played)&&<p>CLB không tham dự Premier League mùa này được giữ dưới dạng thiếu dữ liệu.</p>}</Card></div>
 <Card title="30 thương vụ lớn nhất" subtitle="Ba mùa 2022–2025"><Table rows={data.top_transfers.map(r=>({...r,fee_mil:Number(r.fee_eur)/1e6}))} columns={[["player_name","Cầu thủ"],["season","Mùa"],["from_club_name","CLB bán"],["to_club_name","CLB mua"],["fee_mil","Phí · M€"]]}/></Card>
 <details className="a-card"><summary>Hồ sơ CLB qua các mùa tham dự</summary><Table rows={Object.values(data.club_profiles)} columns={[["club_name","CLB"],["seasons_played","Số mùa"],["avg_points","Điểm TB"],["total_spend","Chi mua · triệu EUR"],["total_income","Thu bán · triệu EUR"],["cost_per_point","Triệu EUR / điểm"]]}/></details></>;
}
