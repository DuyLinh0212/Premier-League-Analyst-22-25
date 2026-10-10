"use client";
import { useEffect, useState } from "react";

const api = async (path, options) => {
  const r = await fetch(`/api/football/${path}`, options);
  const data = await r.json();
  if (!r.ok) throw new Error(typeof data.detail === "string" ? data.detail : (data.detail || []).map(x => x.msg).join("; "));
  return data;
};
const percent = x => `${(100 * Number(x)).toFixed(1)}%`;
const initial = { possession: 50, shots: 12, shots_on_target: 4 };
const algorithmName = key => ({ random_forest: "Random Forest", knn_classifier: "KNN", logistic_regression: "Logistic Regression" }[key] || "Logistic Regression");

export default function Football() {
  const [tab, setTab] = useState("match"), [data, setData] = useState(null), [error, setError] = useState("");
  const [home, setHome] = useState("ARS"), [away, setAway] = useState("CHE"), [sample, setSample] = useState("");
  const [stats, setStats] = useState({ home: { ...initial }, away: { ...initial } });
  const [result, setResult] = useState(null), [busy, setBusy] = useState(false);
  const [club, setClub] = useState("ARS"), [neighbors, setNeighbors] = useState([]);
  useEffect(() => {
    Promise.all([api("features"), api("matches"), api("clubs"), api("model/metrics"), api("knn/predictions")])
      .then(([features, matches, clubs, metrics, knn]) => setData({ features, matches, clubs, metrics, knn }))
      .catch(e => setError(e.message));
  }, []);
  useEffect(() => {
    if (data) api(`knn/neighbors/${club}`).then(setNeighbors).catch(e => setError(e.message));
  }, [club, data]);
  function chooseSample(id) {
    const row = data.matches.find(r => r.match_id === id);
    setSample(id); setResult(null); setError("");
    if (row) { setHome(row.home_team); setAway(row.away_team); setStats({ home: row.home_stats, away: row.away_stats }); }
  }
  function update(side, key, value) {
    setStats(s => ({ ...s, [side]: { ...s[side], [key]: key === "formation" ? value || null : value === "" ? null : Number(value) } }));
    setSample(""); setResult(null);
  }
  async function submit(event) {
    event.preventDefault(); setBusy(true); setError(""); setResult(null);
    if (home === away) { setError("Chọn hai CLB khác nhau."); setBusy(false); return; }
    for (const side of ["home", "away"]) {
      if (Number(stats[side].shots_on_target) > Number(stats[side].shots)) {
        setError("Sút trúng đích không được vượt tổng số sút."); setBusy(false); return;
      }
    }
    try { setResult(await api("predict", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ home_team: home, away_team: away, home_stats: stats.home, away_stats: stats.away }) })); }
    catch (e) { setError(e.message); }
    finally { setBusy(false); }
  }
  const name = id => data?.clubs.find(c => c.id === id)?.name || id;
  const knn = data?.knn.find(r => r.query_club_id === club);
  return <section className="football a-match">
    <header className="f-header"><span className="a-match-label">Phân tích trận đấu</span>
      <nav aria-label="Phân tích"><button aria-pressed={tab === "match"} onClick={() => { setTab("match"); setError(""); }}>Dự đoán trận</button><button aria-pressed={tab === "knn"} onClick={() => { setTab("knn"); setError(""); }}>Đội tương đồng · KNN</button></nav>
    </header>
    {!data ? <section className="f-loading"><h1>{error ? "Chưa tải được dữ liệu" : "Đang đọc dữ liệu trận…"}</h1>{error && <><p role="alert">{error}</p><button onClick={() => location.reload()}>Tải lại dữ liệu</button></>}</section> : <>
      <div className="f-intro"><div><span className="f-eyebrow">{tab === "match" ? "THỐNG KÊ HAI ĐỘI → BA XÁC SUẤT" : "PHONG ĐỘ TƯƠNG ĐỒNG QUA CÁC MÙA"}</span><h1>{tab === "match" ? "Dự đoán trận đấu" : "Đội tương đồng · KNN"}</h1><p>{tab === "match" ? "Điều chỉnh số liệu chủ nhà và đội khách để xem xác suất thắng, hòa và thua." : "So sánh phong độ mùa 2024–2025 với những đội ở hai mùa trước."}</p></div><div className="f-source">1.140 trận thật<br/><span>2022–2023 → 2024–2025</span></div></div>
      {error && <div role="alert" className="f-error">{error}</div>}
      {tab === "match" ? <>
        <form onSubmit={submit} className="f-workspace">
          <section className="f-inputs"><label className="f-sample">Bắt đầu từ một trận có dữ liệu<select value={sample} onChange={e => chooseSample(e.target.value)}><option value="">Nhập thống kê của bạn</option>{data.matches.slice().reverse().map(r => <option key={r.match_id} value={r.match_id}>{r.match_date} · {r.home_name} — {r.away_name}</option>)}</select></label>
            <div className="f-team-head"><label>CHỦ NHÀ<select aria-label="Đội chủ nhà" value={home} onChange={e => { setHome(e.target.value); setSample(""); setResult(null); }}>{data.clubs.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}</select></label><span>VS</span><label>ĐỘI KHÁCH<select aria-label="Đội khách" value={away} onChange={e => { setAway(e.target.value); setSample(""); setResult(null); }}>{data.clubs.map(c => <option key={c.id} value={c.id}>{c.name}</option>)}</select></label></div>
            <div className="f-legend"><span>Chủ nhà</span><span>THỐNG KÊ</span><span>Đội khách</span></div>
            {data.features.metrics.map(m => <div className={`f-stat-row ${!m.used_by_model ? "f-unused" : ""}`} key={m.key}>{["home", "away"].map((side, i) => <span className={`f-stat-cell f-${side}`} key={side}>
              {i === 1 && <span className="f-stat-title">{m.label}<small>{m.unit}{m.required ? " · bắt buộc" : !m.used_by_model ? " · thiếu dữ liệu huấn luyện" : ""}</small></span>}
              <input aria-label={`${side === "home" ? "Chủ nhà" : "Đội khách"} ${m.label}`} id={`${side}-${m.key}`} type="number" min="0" max={m.max ?? undefined} step={m.unit === "%" ? "0.1" : "1"} required={m.required} disabled={!m.used_by_model} placeholder="Để trống" value={stats[side][m.key] ?? ""} onChange={e => update(side, m.key, e.target.value)} />
              {i === 0 && <span className="f-stat-title">{m.label}<small>{m.unit}{m.required ? " · bắt buộc" : !m.used_by_model ? " · thiếu dữ liệu huấn luyện" : ""}</small></span>}
            </span>)}</div>)}
            <div className="f-formation">{["home", "away"].map(side => <label key={side}>Đội hình {side === "home" ? "chủ nhà" : "đội khách"}<input placeholder="Ví dụ 4-3-3" maxLength={30} value={stats[side].formation ?? ""} onChange={e => update(side, "formation", e.target.value)} /></label>)}</div>
            <p className="f-footnote">Trường bổ sung để trống được điền theo median tập huấn luyện. Bộ dữ liệu chưa có số lần chuyền bóng hoặc thống kê theo phút.</p>
            <button className="f-submit" type="submit" disabled={busy}>{busy ? "Đang ước tính…" : "Ước tính kết quả →"}</button>
          </section>
          <aside className="f-result" aria-live="polite"><span className="f-eyebrow">XÁC SUẤT KẾT QUẢ</span><h2>{name(home)}<span>gặp</span>{name(away)}</h2>
            {result ? <><div className="f-prob-track">{Object.entries(result.probabilities).map(([k, v]) => <span key={k} className={k} style={{ width: `${v * 100}%` }} />)}</div><div className="f-prob-values">{[["home_win", "Chủ nhà thắng"], ["draw", "Hòa"], ["away_win", "Đội khách thắng"]].map(([k, label]) => <div key={k} className={k}><strong>{percent(result.probabilities[k])}</strong><span>{label}</span></div>)}</div><p className="f-model-id">Model: {result.model_version}</p>{result.imputed_fields.length > 0 && <details><summary>Đã điền {result.imputed_fields.length} trường thiếu</summary><p>{result.imputed_fields.join(", ")}</p></details>}<p className="f-saved">Đã lưu kết quả phân tích vào Neo4j.</p></> : <div className="f-empty"><div className="f-pitch"><span>H</span><i/><span>A</span></div><p>Nhập thống kê hoặc chọn trận mẫu,<br/>rồi bấm “Ước tính kết quả”.</p></div>}
            <div className="f-method"><strong>Ước tính từ thống kê tổng kết trận</strong><p>Chưa phải dự đoán theo phút. Tỉ số và kết quả thật không được dùng làm đầu vào.</p></div>
          </aside>
        </form>
        <section className="f-evaluation"><h2>Model làm được đến đâu?</h2><p><strong>{algorithmName(data.metrics.algorithm)}</strong> · Kiểm thử riêng trên 380 trận mùa 2024–2025, sau khi chọn và hiệu chỉnh model bằng hai mùa trước.</p><div className="f-eval-grid"><div><strong>{percent(data.metrics.test.accuracy)}</strong><span>Accuracy của model</span></div><div><strong>{percent(data.metrics.baseline.accuracy)}</strong><span>Accuracy baseline</span></div><div><strong>{data.metrics.test.log_loss.toFixed(3)}</strong><span>Log loss · thấp hơn tốt hơn</span></div><div><strong>{data.metrics.test.macro_f1.toFixed(3)}</strong><span>Macro-F1</span></div></div><p className="f-footnote">F1 lớp hòa = {data.metrics.test.class_f1.draw.toFixed(3)}.{data.metrics.test.class_f1.draw < 0.2 ? " Khả năng nhận diện trận hòa còn yếu." : ""} Xác suất là ước tính từ dữ liệu, không phải bảo đảm kết quả.</p>{data.metrics.comparison && <details><summary>So sánh KNN, Logistic Regression và Random Forest</summary><p>Chọn cấu hình có log loss thấp nhất trên 190 trận validation. Các con số dưới đây thuộc validation; mùa kiểm thử không dùng để chọn model.</p><div className="f-table-scroll"><table><thead><tr><th>Thuật toán</th><th>Cấu hình</th><th>Log loss</th><th>Accuracy</th></tr></thead><tbody>{data.metrics.comparison.slice().sort((a,b) => a.validation_log_loss-b.validation_log_loss).map((r,i) => <tr key={i}><td>{algorithmName(r.algorithm)}</td><td>{r.algorithm === "random_forest" ? `64 cây · sâu ${r.parameters.max_depth}${r.parameters.balanced ? " · cân bằng lớp" : ""}` : r.algorithm === "knn_classifier" ? `k=${r.parameters.k}` : `reg=${r.parameters.reg_param}`}</td><td>{r.validation_log_loss.toFixed(3)}</td><td>{percent(r.validation_accuracy)}</td></tr>)}</tbody></table></div></details>}<details><summary>Xem confusion matrix và Brier score</summary><p>Brier score: {data.metrics.test.brier_score.toFixed(3)}</p><table><thead><tr><th>Thực tế / Dự đoán</th><th>Chủ nhà thắng</th><th>Hòa</th><th>Đội khách thắng</th></tr></thead><tbody>{data.metrics.test.confusion_matrix.map((row, i) => <tr key={i}><th>{["Chủ nhà thắng", "Hòa", "Đội khách thắng"][i]}</th>{row.map((n, j) => <td key={j}>{n}</td>)}</tr>)}</tbody></table></details></section>
      </> : <section className="f-knn"><label>Chọn CLB mùa 2024–2025<select value={club} onChange={e => setClub(e.target.value)}>{data.knn.map(r => <option key={r.query_club_id} value={r.query_club_id}>{r.query_club_name}</option>)}</select></label>{knn && <><div className="f-knn-summary"><h2>{knn.query_club_name}</h2><div><strong>{Number(knn.predicted_rank).toFixed(1)}</strong><span>Hạng trung bình láng giềng</span></div><div><strong>{percent(knn.top4_probability)}</strong><span>Tỷ lệ láng giềng Top 4</span></div></div><h3>5 đội / mùa gần nhất</h3><div className="f-table-scroll"><table><thead><tr><th>Thứ tự</th><th>CLB</th><th>Mùa</th><th>Khoảng cách</th><th>Hạng thật</th></tr></thead><tbody>{neighbors.map((r, i) => <tr key={i}><td>{r.neighbor_order}</td><td>{r.neighbor_club_name}</td><td>{r.neighbor_season}</td><td>{Number(r.distance).toFixed(3)}</td><td>{r.neighbor_rank}</td></tr>)}</tbody></table></div><p className="f-footnote">KNN dùng 5 chỉ số phong độ đã chuẩn hóa và khoảng cách Euclid. Tỷ lệ Top 4 là tỷ lệ trong nhóm láng giềng. Các mùa giả lập không được đưa vào phân tích.</p></>}</section>}
      <footer className="f-footer">Nguồn: FBref · Ba mùa lịch sử · KNN + Spark MLlib</footer>
    </>}
  </section>;
}
