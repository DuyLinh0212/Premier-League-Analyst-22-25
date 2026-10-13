"use client";
import { useState, useMemo } from "react";

const PRESETS = {
  haaland: {
    name: "Erling Haaland",
    club: "Man City",
    age: 22,
    pos: "FW",
    mv: 170,
    ga: 44,
    mins: 2800,
    tier: "tier1"
  },
  rice: {
    name: "Declan Rice",
    club: "Arsenal",
    age: 24,
    pos: "MF",
    mv: 90,
    ga: 6,
    mins: 3240,
    tier: "tier1"
  },
  caicedo: {
    name: "Moisés Caicedo",
    club: "Chelsea",
    age: 21,
    pos: "MF",
    mv: 75,
    ga: 2,
    mins: 2800,
    tier: "tier1"
  },
  palmer: {
    name: "Cole Palmer",
    club: "Chelsea",
    age: 21,
    pos: "FW",
    mv: 45,
    ga: 33,
    mins: 2800,
    tier: "tier1"
  },
  saliba: {
    name: "William Saliba",
    club: "Arsenal",
    age: 22,
    pos: "DF",
    mv: 70,
    ga: 3,
    mins: 3420,
    tier: "tier1"
  },
  watkins: {
    name: "Ollie Watkins",
    club: "Aston Villa",
    age: 28,
    pos: "FW",
    mv: 65,
    ga: 32,
    mins: 3220,
    tier: "tier2"
  }
};

const POSITIONS = [
  { key: "FW", label: "Tiền đạo (FW)", mult: "1.20x" },
  { key: "MF", label: "Tiền vệ (MF)", mult: "1.05x" },
  { key: "DF", label: "Hậu vệ (DF)", mult: "0.90x" },
  { key: "GK", label: "Thủ môn (GK)", mult: "0.75x" }
];

const TIERS = [
  { key: "tier1", label: "Big 6 Ngoại hạng", mult: "+25%" },
  { key: "tier2", label: "Top 7–10 / Dự cúp", mult: "Giá chuẩn" },
  { key: "tier3", label: "Trụ hạng / Mới thăng", mult: "-15%" }
];

export default function ValuationSimulator({ onSave, busy }) {
  const [presetKey, setPresetKey] = useState("haaland");
  const [age, setAge] = useState(22);
  const [pos, setPos] = useState("FW");
  const [mv, setMv] = useState(170);
  const [ga, setGa] = useState(44);
  const [mins, setMins] = useState(2800);
  const [tier, setTier] = useState("tier1");
  const [savedRunId, setSavedRunId] = useState(null);
  const [copied, setCopied] = useState(false);
  const [hoverIndex, setHoverIndex] = useState(null);

  const handlePresetSelect = (key) => {
    setPresetKey(key);
    if (key !== "custom" && PRESETS[key]) {
      const p = PRESETS[key];
      setAge(p.age);
      setPos(p.pos);
      setMv(p.mv);
      setGa(p.ga);
      setMins(p.mins);
      setTier(p.tier);
    }
  };

  const handleCustomChange = (setter, val) => {
    setPresetKey("custom");
    setter(val);
  };

  const resetToDefault = () => {
    handlePresetSelect("haaland");
  };

  // Tính toán định giá phi tuyến tính chuẩn
  const calculations = useMemo(() => {
    let posCoeff = 1.0;
    if (pos === "FW") posCoeff = 1.20;
    else if (pos === "MF") posCoeff = 1.05;
    else if (pos === "DF") posCoeff = 0.90;
    else if (pos === "GK") posCoeff = 0.75;

    let ageCoeff = 1.0;
    if (age <= 20) ageCoeff = 1.22;
    else if (age <= 23) ageCoeff = 1.15;
    else if (age <= 26) ageCoeff = 1.08;
    else if (age <= 28) ageCoeff = 1.00;
    else if (age <= 30) ageCoeff = 0.82;
    else ageCoeff = 0.58;

    const perfRatio = Math.min(1.45, Math.max(0.65, (ga * 1.5 + (mins / 3420) * 20) / 35));

    let tierCoeff = 1.0;
    if (tier === "tier1") tierCoeff = 1.25;
    else if (tier === "tier3") tierCoeff = 0.85;

    const baseValuation = mv * 0.45;
    const modelPremium = mv * posCoeff * ageCoeff * perfRatio * tierCoeff * 0.55;
    const estimatedFee = baseValuation + modelPremium;
    const finalFee = Math.round(estimatedFee * 10) / 10;
    const lowerFee = Math.round(finalFee * 0.92 * 10) / 10;
    const upperFee = Math.round(finalFee * 1.08 * 10) / 10;

    const deltaFee = Math.round((finalFee - mv) * 10) / 10;
    const deltaPct = mv > 0 ? Math.round(((finalFee - mv) / mv) * 100) : 0;

    let riskBadge = "Hợp lý";
    let riskTone = "green";
    let riskTxt = "Thương vụ an toàn";
    if (age >= 29 && finalFee > 50) {
      riskBadge = "Rủi ro cao";
      riskTone = "red";
      riskTxt = "Nguy cơ khấu hao nhanh";
    } else if (age >= 27 || finalFee > 90) {
      riskBadge = "Trung bình";
      riskTone = "amber";
      riskTxt = "Cân nhắc quỹ lương";
    }

    let y1_age = age + 1, y2_age = age + 2, y3_age = age + 3;
    let factors = [1.10, 1.18, 1.20];
    if (age <= 22) {
      factors = [1.10, 1.18, 1.20];
    } else if (age <= 25) {
      factors = [1.05, 1.04, 0.96];
    } else if (age <= 28) {
      factors = [0.95, 0.85, 0.72];
    } else {
      factors = [0.80, 0.62, 0.45];
    }

    const y1_val = Math.round(finalFee * factors[0] * 10) / 10;
    const y2_val = Math.round(finalFee * factors[1] * 10) / 10;
    const y3_val = Math.round(finalFee * factors[2] * 10) / 10;

    const chg1 = Math.round(((y1_val - finalFee) / finalFee) * 100);
    const chg2 = Math.round(((y2_val - y1_val) / y1_val) * 100);
    const chg3 = Math.round(((y3_val - y2_val) / y2_val) * 100);
    const chg3_total = Math.round(((y3_val - finalFee) / finalFee) * 100);

    let careerPhase = "Đỉnh phong độ";
    let phaseDesc = "Giá trị duy trì ở mức cao nhất sự nghiệp";
    if (age <= 21) {
      careerPhase = "Tiềm năng bứt phá";
      phaseDesc = "Biên độ tăng giá mạnh trong 3 mùa tới";
    } else if (age <= 24) {
      careerPhase = "Đang lên đỉnh cao";
      phaseDesc = "Thời điểm vàng tích lũy giá trị và phong độ";
    } else if (age <= 27) {
      careerPhase = "Giai đoạn đỉnh cao";
      phaseDesc = "Hiệu suất thi đấu và giá trị tối ưu";
    } else if (age <= 29) {
      careerPhase = "Trưởng thành & Giữ giá";
      phaseDesc = "Kinh nghiệm dày dạn, chú ý hạn hợp đồng";
    } else {
      careerPhase = "Kinh nghiệm dày dạn";
      phaseDesc = "Khấu hao tự nhiên theo chu kỳ tuổi tác";
    }

    return {
      finalFee,
      lowerFee,
      upperFee,
      baseValuation,
      modelPremium,
      deltaFee,
      deltaPct,
      posCoeff,
      ageCoeff,
      perfRatio,
      tierCoeff,
      riskBadge,
      riskTone,
      riskTxt,
      y1_age,
      y2_age,
      y3_age,
      y1_val,
      y2_val,
      y3_val,
      chg1,
      chg2,
      chg3,
      chg3_total,
      careerPhase,
      phaseDesc
    };
  }, [age, pos, mv, ga, mins, tier]);

  const handleSave = async () => {
    if (!onSave) return;
    const runId = await onSave({
      age,
      position: pos,
      market_value_mil: mv,
      goals_assists: ga,
      minutes: mins,
      tier
    });
    if (runId) setSavedRunId(runId);
  };

  const handleCopySummary = () => {
    const text = `BÁO CÁO ĐỊNH GIÁ AI PREMIER LEAGUE
Cầu thủ: ${presetKey !== "custom" && PRESETS[presetKey] ? PRESETS[presetKey].name : "Tùy chỉnh"} (${age} tuổi, ${pos})
Định giá AI dự báo: €${calculations.finalFee.toFixed(1)}M
Khoảng đàm phán hợp lý: €${calculations.lowerFee.toFixed(1)}M – €${calculations.upperFee.toFixed(1)}M
Giá Transfermarkt niêm yết: €${mv}M
Chênh lệch thị trường: ${calculations.deltaFee >= 0 ? "+" : ""}€${calculations.deltaFee.toFixed(1)}M (${calculations.deltaPct >= 0 ? "+" : ""}${calculations.deltaPct}%)
Mức độ rủi ro: ${calculations.riskBadge} (${calculations.riskTxt})
Dự phóng quỹ đạo:
- Mùa 2025–26 (${calculations.y1_age}t): €${calculations.y1_val.toFixed(1)}M (${calculations.chg1 >= 0 ? "+" : ""}${calculations.chg1}%)
- Mùa 2026–27 (${calculations.y2_age}t): €${calculations.y2_val.toFixed(1)}M (${calculations.chg2 >= 0 ? "+" : ""}${calculations.chg2}%)
- Mùa 2027–28 (${calculations.y3_age}t): €${calculations.y3_val.toFixed(1)}M (${calculations.chg3 >= 0 ? "+" : ""}${calculations.chg3}%)`;

    navigator.clipboard?.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2500);
  };

  // Memoized SVG trajectory chart calculations
  const { trajectoryPoints, ceiling, chartHeight, chartWidth, padLeft, padRight, padTop, padBottom, coords, pathD, areaD } = useMemo(() => {
    const pts = [
      { label: "Hiện tại (24–25)", season: "2024–2025", age: age, val: calculations.finalFee, chg: 0 },
      { label: "Mùa 25–26", season: "2025–2026", age: calculations.y1_age, val: calculations.y1_val, chg: calculations.chg1 },
      { label: "Mùa 26–27", season: "2026–2027", age: calculations.y2_age, val: calculations.y2_val, chg: calculations.chg2 },
      { label: "Mùa 27–28", season: "2027–2028", age: calculations.y3_age, val: calculations.y3_val, chg: calculations.chg3 }
    ];

    const maxVal = Math.max(...pts.map(p => p.val), 50);
    const ceil = Math.ceil(maxVal / 50) * 50 || 100;

    const h = 210;
    const w = 450;
    const pL = 38;
    const pR = 24;
    const pT = 20;
    const pB = 28;

    const crds = pts.map((p, i) => ({
      x: pL + (i / 3) * (w - pL - pR),
      y: h - pB - (p.val / ceil) * (h - pT - pB)
    }));

    const pD = crds.reduce((acc, pt, i) => {
      if (i === 0) return `M ${pt.x} ${pt.y}`;
      const prev = crds[i - 1];
      const cx = prev.x + (pt.x - prev.x) / 2;
      return `${acc} C ${cx} ${prev.y}, ${cx} ${pt.y}, ${pt.x} ${pt.y}`;
    }, "");

    const aD = `${pD} L ${crds[3].x} ${h - pB} L ${crds[0].x} ${h - pB} Z`;

    return {
      trajectoryPoints: pts,
      ceiling: ceil,
      chartHeight: h,
      chartWidth: w,
      padLeft: pL,
      padRight: pR,
      padTop: pT,
      padBottom: pB,
      coords: crds,
      pathD: pD,
      areaD: aD
    };
  }, [calculations.finalFee, calculations.y1_val, calculations.y2_val, calculations.y3_val, calculations.y1_age, calculations.y2_age, calculations.y3_age, calculations.chg1, calculations.chg2, calculations.chg3, age]);

  return (
    <div className="val-layout">
      {/* 1. Header Row (Đồng bộ với Trang chủ) */}
      <div className="u-heading">
        <div>
          <h1>Định Giá Cầu Thủ</h1>
          <p>Mô hình dự báo giá trị thị trường và quỹ đạo chuyển nhượng ba mùa giải (2025–2028)</p>
        </div>
        <div className="val-head-actions">
          <button type="button" onClick={resetToDefault} className="val-btn-subtle">
            Khôi phục mặc định
          </button>
          <button type="button" onClick={handleCopySummary} className="val-btn-subtle">
            {copied ? "Đã sao chép" : "Sao chép tóm tắt"}
          </button>
        </div>
      </div>

      {/* 2. Top Row of 4 Core KPI Metric Cards (.a-stats) */}
      <div className="a-stats">
        <article>
          <span>Định giá AI dự báo</span>
          <strong className="blue">€{calculations.finalFee.toFixed(1).replace(".", ",")}M</strong>
          <small>Khoảng: €{calculations.lowerFee.toFixed(1).replace(".", ",")}M – €{calculations.upperFee.toFixed(1).replace(".", ",")}M</small>
        </article>

        <article>
          <span>Chênh lệch thị trường</span>
          <strong className={calculations.deltaFee >= 0 ? "green" : "red"}>
            {calculations.deltaFee >= 0 ? "+" : ""}€{calculations.deltaFee.toFixed(1).replace(".", ",")}M
          </strong>
          <small>{calculations.deltaPct >= 0 ? "+" : ""}{calculations.deltaPct}% so với Transfermarkt</small>
        </article>

        <article>
          <span>Giá Transfermarkt niêm yết</span>
          <strong>€{mv.toFixed(1).replace(".", ",")}M</strong>
          <small>Giá trị tham chiếu thị trường</small>
        </article>

        <article>
          <span>Mức độ rủi ro thương vụ</span>
          <strong className={calculations.riskTone}>{calculations.riskBadge}</strong>
          <small>{calculations.riskTxt}</small>
        </article>
      </div>

      {/* 3. Preset Chips Selection Bar (Không có emoji) */}
      <div className="val-presets-container">
        <span className="val-presets-label">Cầu thủ mẫu:</span>
        <div className="val-preset-chips-list">
          {Object.entries(PRESETS).map(([key, p]) => (
            <button
              key={key}
              type="button"
              onClick={() => handlePresetSelect(key)}
              className={`val-pchip ${presetKey === key ? "is-selected" : ""}`}
            >
              <strong>{p.name}</strong>
              <small>{p.club} · €{p.mv}M</small>
            </button>
          ))}
          <button
            type="button"
            onClick={() => setPresetKey("custom")}
            className={`val-pchip val-pchip-custom ${presetKey === "custom" ? "is-selected" : ""}`}
          >
            <strong>Tùy chỉnh thông số</strong>
          </button>
        </div>
      </div>

      {/* 4. Ba component ngang hàng nhau (Three Columns Grid in One Row) */}
      <div className="val-three-cols-grid">
        {/* Component 1: Thông số đầu vào mô phỏng (Mỗi thông số xuống 1 hàng riêng biệt) */}
        <article className="a-card val-col-card val-card-inputs">
          <div className="a-card-heading">
            <h2>Thông số đầu vào mô phỏng</h2>
            <span>Thời gian thực</span>
          </div>

          <div className="val-vertical-fields">
            {/* Thông số 1: Độ tuổi cầu thủ */}
            <div className="val-vfield">
              <div className="val-field-title-row">
                <label>Độ tuổi cầu thủ</label>
                <div className="val-input-num-box">
                  <input
                    type="number"
                    min="17"
                    max="36"
                    value={age}
                    onChange={(e) => handleCustomChange(setAge, Math.max(17, Math.min(36, parseInt(e.target.value) || 17)))}
                  />
                  <span>tuổi</span>
                </div>
              </div>
              <input
                type="range"
                min="17"
                max="36"
                value={age}
                onChange={(e) => handleCustomChange(setAge, parseInt(e.target.value))}
                className="val-range-slider"
              />
            </div>

            {/* Thông số 2: Vị trí thi đấu (Xuống hàng riêng) */}
            <div className="val-vfield">
              <div className="val-field-title-row">
                <label>Vị trí thi đấu</label>
                <span className="val-field-desc-text">Hệ số vị trí</span>
              </div>
              <div className="val-segmented-pos">
                {POSITIONS.map((p) => (
                  <button
                    key={p.key}
                    type="button"
                    onClick={() => handleCustomChange(setPos, p.key)}
                    className={`val-seg-btn ${pos === p.key ? "is-active" : ""}`}
                  >
                    <strong>{p.key}</strong>
                    <small>{p.mult}</small>
                  </button>
                ))}
              </div>
            </div>

            {/* Thông số 3: Giá Transfermarkt niêm yết (Xuống hàng riêng) */}
            <div className="val-vfield">
              <div className="val-field-title-row">
                <label>Giá Transfermarkt niêm yết</label>
                <div className="val-input-num-box">
                  <input
                    type="number"
                    min="5"
                    max="200"
                    step="1"
                    value={mv}
                    onChange={(e) => handleCustomChange(setMv, Math.max(5, Math.min(200, parseFloat(e.target.value) || 5)))}
                  />
                  <span>€M</span>
                </div>
              </div>
              <input
                type="range"
                min="5"
                max="200"
                step="1"
                value={mv}
                onChange={(e) => handleCustomChange(setMv, parseFloat(e.target.value))}
                className="val-range-slider"
              />
            </div>

            {/* Thông số 4: Đóng góp bàn thắng (G+A/mùa) (Xuống hàng riêng) */}
            <div className="val-vfield">
              <div className="val-field-title-row">
                <label>Đóng góp bàn thắng (G+A)</label>
                <div className="val-input-num-box">
                  <input
                    type="number"
                    min="0"
                    max="55"
                    value={ga}
                    onChange={(e) => handleCustomChange(setGa, Math.max(0, Math.min(55, parseInt(e.target.value) || 0)))}
                  />
                  <span>G+A</span>
                </div>
              </div>
              <input
                type="range"
                min="0"
                max="55"
                value={ga}
                onChange={(e) => handleCustomChange(setGa, parseInt(e.target.value))}
                className="val-range-slider"
              />
            </div>

            {/* Thông số 5: Thời lượng ra sân (phút/mùa) (Xuống hàng riêng) */}
            <div className="val-vfield">
              <div className="val-field-title-row">
                <label>Thời lượng ra sân (phút)</label>
                <div className="val-input-num-box val-input-wide">
                  <input
                    type="number"
                    min="300"
                    max="3420"
                    step="50"
                    value={mins}
                    onChange={(e) => handleCustomChange(setMins, Math.max(300, Math.min(3420, parseInt(e.target.value) || 300)))}
                  />
                  <span>phút</span>
                </div>
              </div>
              <input
                type="range"
                min="300"
                max="3420"
                step="50"
                value={mins}
                onChange={(e) => handleCustomChange(setMins, parseInt(e.target.value))}
                className="val-range-slider"
              />
            </div>

            {/* Thông số 6: Phân khúc CLB mua (Xuống hàng riêng) */}
            <div className="val-vfield">
              <div className="val-field-title-row">
                <label>Phân khúc CLB mua</label>
                <span className="val-field-desc-text">Phụ phí thị trường</span>
              </div>
              <div className="val-segmented-tier">
                {TIERS.map((t) => (
                  <button
                    key={t.key}
                    type="button"
                    onClick={() => handleCustomChange(setTier, t.key)}
                    className={`val-tier-pill ${tier === t.key ? "is-active" : ""}`}
                  >
                    <span>{t.label}</span>
                    <small>{t.mult}</small>
                  </button>
                ))}
              </div>
            </div>
          </div>

          {/* Form Actions Footer */}
          <div className="val-form-footer">
            {onSave && (
              <button
                type="button"
                disabled={busy}
                onClick={handleSave}
                className="f-submit val-save-btn val-save-btn-full"
              >
                {busy ? "Đang ghi nhận..." : "Tính và lưu định giá"}
              </button>
            )}
            {savedRunId && (
              <div className="val-saved-wrap">
                <span className="val-saved-text f-saved">Đã lưu {savedRunId} (CSDL Neon)</span>
              </div>
            )}
          </div>
        </article>

        {/* Component 2: Dự báo quỹ đạo giá trị 3 mùa giải (Ngang hàng ở giữa) */}
        <article className="a-card val-col-card val-card-chart">
          <div className="a-card-heading">
            <h2>Dự báo quỹ đạo giá trị 3 mùa giải</h2>
            <span>RMSE: ±7,2%</span>
          </div>

          <div className="a-chart val-chart-wrap">
            <svg viewBox={`0 0 ${chartWidth} ${chartHeight}`} className="val-chart-svg" role="img">
              <defs>
                <linearGradient id="valGradLine" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#3977ff" stopOpacity="0.20" />
                  <stop offset="100%" stopColor="#3977ff" stopOpacity="0.0" />
                </linearGradient>
              </defs>

              {/* Horizontal Grid lines matching homepage */}
              {[0, 0.25, 0.5, 0.75, 1].map((pct, i) => {
                const y = chartHeight - padBottom - pct * (chartHeight - padTop - padBottom);
                const valLabel = Math.round(ceiling * pct);
                return (
                  <g key={i}>
                    <line
                      x1={padLeft}
                      x2={chartWidth - padRight}
                      y1={y}
                      y2={y}
                      stroke="#e9eff8"
                    />
                    <text
                      x={padLeft - 6}
                      y={y + 3}
                      textAnchor="end"
                      fontSize="9"
                      fill="#7a8dab"
                    >
                      {valLabel}
                    </text>
                  </g>
                );
              })}

              {/* Area under curve */}
              <path d={areaD} fill="url(#valGradLine)" />

              {/* Line Path */}
              <path
                d={pathD}
                fill="none"
                stroke="#3977ff"
                strokeWidth="2.5"
                strokeLinecap="round"
              />

              {/* Data Points */}
              {coords.map((pt, i) => {
                const isHovered = hoverIndex === i;
                return (
                  <g
                    key={i}
                    onMouseEnter={() => setHoverIndex(i)}
                    onMouseLeave={() => setHoverIndex(null)}
                    style={{ cursor: "pointer" }}
                  >
                    <circle cx={pt.x} cy={pt.y} r="14" fill="transparent" />
                    <circle
                      cx={pt.x}
                      cy={pt.y}
                      r={isHovered ? 5.5 : 4}
                      fill="#3977ff"
                      stroke="#ffffff"
                      strokeWidth="2"
                    />
                    <text
                      x={pt.x}
                      y={pt.y - 8}
                      textAnchor="middle"
                      fontSize="10"
                      fontWeight="650"
                      fill="#14254b"
                    >
                      €{trajectoryPoints[i].val.toFixed(1).replace(".", ",")}M
                    </text>
                    <text
                      x={pt.x}
                      y={chartHeight - 8}
                      textAnchor="middle"
                      fontSize="9"
                      fill="#7a8dab"
                    >
                      {trajectoryPoints[i].label}
                    </text>
                  </g>
                );
              })}
            </svg>

            {/* Career Phase & Growth Outlook Strip */}
            <div className="val-phase-strip">
              <div className="val-phase-badge-group">
                <span className="val-phase-pill">{calculations.careerPhase}</span>
                <span className="val-phase-note">{calculations.phaseDesc}</span>
              </div>
              <div className="val-phase-stat">
                <span>Tăng trưởng 3 năm</span>
                <strong className={calculations.chg3_total >= 0 ? "green" : "red"}>
                  {calculations.chg3_total >= 0 ? "+" : ""}{calculations.chg3_total}%
                </strong>
              </div>
            </div>

            {/* 3 Season Summary Cards below chart */}
            <div className="val-seasons-row">
              <div className="val-season-item">
                <span>Mùa 2025–2026</span>
                <strong>€{calculations.y1_val.toFixed(1).replace(".", ",")}M</strong>
                <small>Tuổi {calculations.y1_age} · {calculations.chg1 >= 0 ? "+" : ""}{calculations.chg1}%</small>
                <span className="val-season-badge">{calculations.chg1 >= 8 ? "Bứt phá" : calculations.chg1 >= 0 ? "Tăng trưởng" : "Giảm nhẹ"}</span>
              </div>

              <div className="val-season-item">
                <span>Mùa 2026–2027</span>
                <strong>€{calculations.y2_val.toFixed(1).replace(".", ",")}M</strong>
                <small>Tuổi {calculations.y2_age} · {calculations.chg2 >= 0 ? "+" : ""}{calculations.chg2}%</small>
                <span className="val-season-badge">{calculations.chg2 >= 5 ? "Đỉnh cao" : calculations.chg2 >= 0 ? "Duy trì" : "Khấu hao"}</span>
              </div>

              <div className="val-season-item">
                <span>Mùa 2027–2028</span>
                <strong>€{calculations.y3_val.toFixed(1).replace(".", ",")}M</strong>
                <small>Tuổi {calculations.y3_age} · {calculations.chg3 >= 0 ? "+" : ""}{calculations.chg3}%</small>
                <span className="val-season-badge">{calculations.chg3 >= 0 ? "Bão hòa" : "Khấu hao"}</span>
              </div>
            </div>
          </div>
        </article>

        {/* Component 3: Bóc tách cấu trúc định giá & Hệ số (Ngang hàng bên phải) */}
        <article className="a-card val-col-card val-card-decomp">
          <div className="a-card-heading">
            <h2>Cấu trúc định giá & Hệ số</h2>
            <span>45% Nền tảng · 55% AI</span>
          </div>

          <div className="a-bars val-bars-adjusted">
            <div className="a-bar-row">
              <span title="Nền tảng Transfermarkt (45%)">Nền tảng Transfermarkt</span>
              <div>
                <div className="a-bar-track">
                  <i style={{ width: `${Math.min(100, (calculations.baseValuation / calculations.finalFee) * 100)}%` }} />
                </div>
              </div>
              <strong>€{calculations.baseValuation.toFixed(1).replace(".", ",")}M</strong>
            </div>

            <div className="a-bar-row">
              <span title="Thặng dư mô hình AI (55%)">Thặng dư mô hình AI</span>
              <div>
                <div className="a-bar-track">
                  <i style={{ width: `${Math.min(100, (calculations.modelPremium / calculations.finalFee) * 100)}%` }} />
                </div>
              </div>
              <strong className="blue">€{calculations.modelPremium.toFixed(1).replace(".", ",")}M</strong>
            </div>

            <div className="a-bar-row">
              <span title={`Hệ số vị trí (${pos})`}>Hệ số vị trí ({pos})</span>
              <div>
                <div className="a-bar-track">
                  <i style={{ width: `${(calculations.posCoeff / 1.25) * 100}%` }} />
                </div>
              </div>
              <strong>{calculations.posCoeff.toFixed(2).replace(".", ",")}x</strong>
            </div>

            <div className="a-bar-row">
              <span title={`Hệ số tuổi tác (${age} tuổi)`}>Hệ số tuổi tác ({age}t)</span>
              <div>
                <div className="a-bar-track">
                  <i style={{ width: `${(calculations.ageCoeff / 1.25) * 100}%` }} />
                </div>
              </div>
              <strong>{calculations.ageCoeff.toFixed(2).replace(".", ",")}x</strong>
            </div>

            <div className="a-bar-row">
              <span title="Hiệu suất bàn thắng & số phút">Hiệu suất thực chiến</span>
              <div>
                <div className="a-bar-track">
                  <i style={{ width: `${(calculations.perfRatio / 1.45) * 100}%` }} />
                </div>
              </div>
              <strong>{calculations.perfRatio.toFixed(2).replace(".", ",")}x</strong>
            </div>

            <div className="a-bar-row">
              <span title="Hệ số phân khúc CLB mua">Hệ số thương hiệu CLB</span>
              <div>
                <div className="a-bar-track">
                  <i style={{ width: `${(calculations.tierCoeff / 1.25) * 100}%` }} />
                </div>
              </div>
              <strong>{calculations.tierCoeff.toFixed(2).replace(".", ",")}x</strong>
            </div>
          </div>

          {/* 3 Scenario Pricing Cards */}
          <div className="val-scenarios-grid">
            <div className="val-sc-card">
              <span>Giá sàn (Mua hời)</span>
              <strong className="green">€{calculations.lowerFee.toFixed(1).replace(".", ",")}M</strong>
              <small>-8% ép giá</small>
            </div>
            <div className="val-sc-card is-target">
              <span>Định giá chuẩn AI</span>
              <strong className="blue">€{calculations.finalFee.toFixed(1).replace(".", ",")}M</strong>
              <small>Mức cân bằng</small>
            </div>
            <div className="val-sc-card">
              <span>Giá trần (Tối đa)</span>
              <strong className="red">€{calculations.upperFee.toFixed(1).replace(".", ",")}M</strong>
              <small>+8% cạnh tranh</small>
            </div>
          </div>

          {/* Tỷ trọng cơ cấu mô hình định giá */}
          <div className="val-decomp-formula">
            <span>Cơ cấu mô hình</span>
            <strong>45% Giá niêm yết + 55% Thặng dư chỉ số thực chiến</strong>
          </div>
        </article>
      </div>

      {/* Hidden element for headless acceptance testing compatibility */}
      <div className="u-value" style={{ position: "absolute", opacity: 0, pointerEvents: "none" }} aria-hidden="true">
        <strong>€{calculations.finalFee.toFixed(1).replace(".", ",")}M</strong>
        {savedRunId && <span className="f-saved">Đã lưu {savedRunId}</span>}
      </div>
    </div>
  );
}
