"use client";
import { useState } from "react";
import { Stats, number } from "./insights";

const riskReasons=p=>(p.reasons||[]).filter(r=>r.includes("giảm")||r.includes("dưới mức"));

export default function OutlookPanel({data,season,Table}){
  const [mode,setMode]=useState("potential");
  const [club,setClub]=useState(""),[risk,setRisk]=useState(""),[selected,setSelected]=useState("");
  const players=data.players||[];
  const clubNames=p=>String(p.clubs||"").split(" / ").map(s=>s.trim()).filter(Boolean);
  const clubs=[...new Set(players.flatMap(clubNames))].sort((a,b)=>a.localeCompare(b));
  const reasons=[...new Set(players.flatMap(p=>riskReasons(p)))].sort((a,b)=>a.localeCompare(b,"vi"));
  const visible=players.filter(p=>(mode==="potential"?p.potential!=null:riskReasons(p).length>0)&&(!club||clubNames(p).includes(club))&&(mode!=="risk"||!risk||(risk==="any"?(riskReasons(p)).length>0:risk==="none"?!(riskReasons(p)).length:(riskReasons(p)).includes(risk))));
  const detail=visible.find(p=>p.player_id===selected);
  return <><div className="u-controls"><label>Chế độ xem<select aria-label="Chế độ xem cầu thủ" value={mode} onChange={e=>{setMode(e.target.value);setRisk("");setSelected("");}}><option value="potential">Tiềm năng</option><option value="risk">Rủi ro</option></select></label><label>CLB<select aria-label="Lọc CLB tiềm năng" value={clubs.includes(club)?club:""} onChange={e=>{setClub(e.target.value);setSelected("");}}><option value="">Tất cả CLB</option>{clubs.map(c=><option key={c} value={c}>{c}</option>)}</select></label>{mode==="risk"&&<label>Rủi ro<select aria-label="Lọc rủi ro cầu thủ" value={risk} onChange={e=>{setRisk(e.target.value);setSelected("");}}><option value="">Tất cả</option><option value="any">Có cảnh báo rủi ro</option>{reasons.map(r=><option key={r} value={r}>{r}</option>)}</select></label>}</div>
    <Stats items={[["Cầu thủ",number(visible.length,0),season,"blue"],["Có cảnh báo",number(visible.filter(p=>(riskReasons(p)).length).length,0),"Theo kết quả đã lưu"],["Tiềm năng cao",number(visible.filter(p=>p.potential>=7).length,0),"Điểm từ 7 / 10","green"]]}/>
    <p>Điểm theo vị trí; giữ cầu thủ ít phút và ghi rõ lịch sử còn thiếu. Rủi ro hiển thị theo cảnh báo có sẵn trong báo cáo. Tuổi trong mùa = năm kết thúc mùa − năm sinh; dữ liệu không có ngày sinh đầy đủ.</p>
    <Table sortable rows={visible.map(p=>({...p,risk_notes:(riskReasons(p)).join(" · ")||"Không có cảnh báo"}))} columns={[["player_name","Cầu thủ"],["clubs","CLB"],["position","Vị trí"],["minutes","Phút"],["potential","Tiềm năng /10"],["age","Tuổi trong mùa"],...(mode==="risk"?[["risk_notes","Rủi ro"]]:[])]}/>
    <label>Chi tiết cầu thủ<select value={detail?selected:""} onChange={e=>setSelected(e.target.value)}><option value="">Chọn cầu thủ</option>{visible.map(p=><option key={p.player_id} value={p.player_id}>{p.player_name}</option>)}</select></label>
    {detail&&<><Table rows={detail.history}/><details><summary>Thành phần điểm và cảnh báo</summary><pre>{JSON.stringify(detail,null,2)}</pre></details></>}
  </>;
}
