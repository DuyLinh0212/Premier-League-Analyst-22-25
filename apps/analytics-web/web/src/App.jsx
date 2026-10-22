import { lazy, Suspense, useEffect, useMemo, useState } from "react";
import {
  Activity,
  AlertCircle,
  BarChart3,
  Cake,
  CalendarDays,
  CircleDollarSign,
  Crosshair,
  Info,
  LoaderCircle,
  Check,
  Search,
  Shield,
  Swords,
  TrendingUp,
  Users,
  Wallet,
  X
} from "lucide-react";
import { apiUrl, getHealth, getJson } from "./api.js";

const Chart = lazy(() => import("./Chart.jsx"));

function ChartPanel(props) {
  return (
    <Suspense fallback={<div className="chart-loading" role="status">Đang tải thư viện biểu đồ…</div>}>
      <Chart {...props} />
    </Suspense>
  );
}

const TABS = [
  { id: "player-radar", label: "So sánh cầu thủ", icon: Users, mode: "dynamic" },
  { id: "player-contribution", label: "Đóng góp cầu thủ", icon: BarChart3, mode: "dynamic" },
  { id: "squad-value", label: "Giá trị đội hình", icon: Wallet, mode: "dynamic" },
  { id: "age-distribution", label: "Độ tuổi", icon: Cake, mode: "static" },
  { id: "head-to-head", label: "Đối đầu", icon: Swords, mode: "dynamic" },
  { id: "matchday-performance", label: "Theo vòng", icon: CalendarDays, mode: "dynamic" },
  { id: "attack-defense", label: "Tấn công · phòng ngự", icon: Crosshair, mode: "static" },
  { id: "points-progression", label: "Diễn biến điểm", icon: TrendingUp, mode: "dynamic" },
  { id: "team-strength", label: "Sức mạnh đội", icon: Shield, mode: "dynamic" },
  { id: "spending-vs-achievement", label: "Chi tiêu · thành tích", icon: CircleDollarSign, mode: "static" }
];

const POSITIONS = {
  DF: "Hậu vệ",
  FW: "Tiền đạo",
  GK: "Thủ môn",
  MF: "Tiền vệ"
};
const PLAYER_RADAR_COLORS = ["#e83267", "#247a5a", "#4f77a8", "#c27a13"];

const money = new Intl.NumberFormat("vi-VN", {
  style: "currency",
  currency: "EUR",
  maximumFractionDigits: 0
});
const integer = new Intl.NumberFormat("vi-VN");
const percent = new Intl.NumberFormat("vi-VN", { maximumFractionDigits: 1 });

function baseChart() {
  return {
    backgroundColor: "transparent",
    animationDuration: 450,
    textStyle: { fontFamily: "Inter, sans-serif", color: "#526176" },
    grid: { left: 60, right: 30, top: 54, bottom: 70, containLabel: true },
    tooltip: { trigger: "axis", borderColor: "#dfe5e9", backgroundColor: "#ffffff", textStyle: { color: "#17243b" } },
    toolbox: { right: 8, feature: { saveAsImage: { title: "Tải ảnh", pixelRatio: 2 } } }
  };
}

function shortSeason(value) {
  if (!value) return "";
  const [start, end] = value.split("-");
  return start?.length === 4 && end?.length === 4 ? `${start}–${end.slice(-2)}` : value.replace("-", "–");
}

function formatEur(value) {
  if (value === null || value === undefined) return "Chưa có định giá";
  if (value >= 1_000_000) return (value / 1_000_000).toLocaleString("vi-VN", { maximumFractionDigits: 1 }) + " triệu €";
  if (value >= 1000) return (value / 1000).toLocaleString("vi-VN", { maximumFractionDigits: 0 }) + " nghìn €";
  return money.format(value);
}

function ErrorState({ message, onRetry }) {
  return (
    <div className="state-card error-state" role="alert">
      <AlertCircle size={20} />
      <div>
        <strong>Chưa tải được dữ liệu</strong>
        <p>{message}</p>
      </div>
      {onRetry && <button className="quiet-button" onClick={onRetry}>Thử lại</button>}
    </div>
  );
}

function LoadingState() {
  return (
    <div className="state-card loading-state" aria-live="polite">
      <LoaderCircle size={19} className="spin" />
      <span>Đang tải dữ liệu phân tích từ PostgreSQL…</span>
    </div>
  );
}

function Insight({ value }) {
  if (!value) return null;
  const kind = value.type || "mô tả";
  const label = kind === "tương quan" ? "TƯƠNG QUAN" : kind === "công thức" ? "CÔNG THỨC" : "MÔ TẢ";
  return (
    <section className="insight-card" aria-label="Kết luận phân tích">
      <div className="insight-symbol"><Activity size={18} /></div>
      <div className="insight-copy">
        <div className="insight-heading">
          <span className="eyebrow">PHÁT HIỆN</span>
          <span className="kind-tag">{label}</span>
          <span className="sample-tag">n = {integer.format(value.sample_size || 0)}</span>
        </div>
        <p className="finding">{value.finding}</p>
        <details>
          <summary><Info size={14} /> Cách tính và giới hạn</summary>
          <div className="method-details">
            <p><strong>Phương pháp:</strong> {value.method}</p>
            <p><strong>Giới hạn:</strong> {value.limitations}</p>
          </div>
        </details>
      </div>
    </section>
  );
}

function SelectField({ label, value, onChange, children, disabled = false }) {
  return (
    <label className="select-field">
      <span>{label}</span>
      <select value={value} onChange={(event) => onChange(event.target.value)} disabled={disabled}>
        {children}
      </select>
    </label>
  );
}

function normalizeSearch(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLocaleLowerCase("vi")
    .replace(/\bmanchester\b/g, "man")
    .trim();
}

function SearchableMultiPicker({
  items,
  selectedIds,
  onChange,
  getId,
  getLabel,
  getSearchText,
  getSubtitle,
  getMeta,
  maxSelections = 4,
  title,
  description,
  placeholder
}) {
  const [query, setQuery] = useState("");
  const selectedItems = selectedIds.map((id) => items.find((item) => getId(item) === id)).filter(Boolean);
  const normalizedQuery = normalizeSearch(query);
  const filteredItems = items.filter((item) => !normalizedQuery || normalizeSearch(getSearchText(item)).includes(normalizedQuery));

  const toggleItem = (item) => {
    const id = getId(item);
    if (selectedIds.includes(id)) {
      onChange(selectedIds.filter((selectedId) => selectedId !== id));
    } else if (selectedIds.length < maxSelections) {
      onChange([...selectedIds, id]);
    }
  };

  const removeItem = (id) => onChange(selectedIds.filter((selectedId) => selectedId !== id));

  return (
    <div className="select-field player-picker">
      <div className="picker-heading">
        <span>{title}</span>
        <span className="picker-count">{selectedIds.length}/{maxSelections}</span>
      </div>
      <span className="picker-description">{description}</span>
      <label className="picker-search">
        <Search size={15} aria-hidden="true" />
        <input
          type="search"
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          placeholder={placeholder}
          aria-label={placeholder}
        />
        {query && (
          <button type="button" onClick={() => setQuery("")} aria-label="Xóa từ khóa tìm kiếm">
            <X size={14} />
          </button>
        )}
      </label>
      {selectedItems.length > 0 ? (
        <div className="picker-selected" aria-label="Các mục đang chọn">
          {selectedItems.map((item) => {
            const id = getId(item);
            return (
              <button
                className="picker-chip"
                key={id}
                type="button"
                onClick={() => removeItem(id)}
                aria-label={`Bỏ chọn ${getLabel(item)}`}
              >
                <span>{getLabel(item)}</span>
                <X size={12} aria-hidden="true" />
              </button>
            );
          })}
        </div>
      ) : (
        <p className="picker-empty-selection">Chưa chọn mục nào.</p>
      )}
      <div className="picker-results" role="group" aria-label="Kết quả lựa chọn">
        <div className="picker-results-heading">
          {normalizedQuery ? `${filteredItems.length} kết quả` : `Danh sách · ${items.length}`}
          {selectedIds.length >= maxSelections && <span>Đã đạt giới hạn</span>}
        </div>
        {filteredItems.length ? filteredItems.map((item) => {
          const id = getId(item);
          const selected = selectedIds.includes(id);
          const disabled = !selected && selectedIds.length >= maxSelections;
          return (
            <button
              className={`picker-option${selected ? " is-selected" : ""}`}
              data-testid={"choice-" + id}
              key={id}
              type="button"
              onClick={() => toggleItem(item)}
              disabled={disabled}
              aria-pressed={selected}
            >
              <span className="picker-check" aria-hidden="true">{selected && <Check size={13} strokeWidth={2.5} />}</span>
              <span className="picker-option-copy">
                <strong>{getLabel(item)}</strong>
                {getSubtitle && <small>{getSubtitle(item)}</small>}
              </span>
              {getMeta && <span className="picker-option-meta">{getMeta(item)}</span>}
            </button>
          );
        }) : (
          <p className="picker-no-results">Không tìm thấy kết quả phù hợp.</p>
        )}
      </div>
    </div>
  );
}

function ControlBar({ activeTab, metadata, season, position, setPosition, clubId, setClubId, radarPlayers, setRadarPlayers, strengthClubs, setStrengthClubs }) {
  const teams = metadata?.teams || [];
  const players = metadata?.players || [];
  const selectedTab = TABS.find((tab) => tab.id === activeTab);
  const clubControl = (
    <SelectField label="Câu lạc bộ" value={clubId} onChange={setClubId} disabled={!teams.length}>
      {teams.map((team) => <option key={team.club_id} value={team.club_id}>{team.club_name}</option>)}
    </SelectField>
  );

  if (activeTab === "player-radar") {
    return (
      <div className="control-bar radar-controls">
        <SelectField label="Vị trí" value={position} onChange={setPosition}>
          {Object.entries(POSITIONS).map(([id, name]) => <option key={id} value={id}>{name}</option>)}
        </SelectField>
        <SearchableMultiPicker
          items={players}
          selectedIds={radarPlayers}
          onChange={setRadarPlayers}
          getId={(player) => player.selection_id}
          getLabel={(player) => player.player_name}
          getSearchText={(player) => `${player.player_name} ${player.club_name}`}
          getSubtitle={(player) => player.club_name}
          getMeta={(player) => `${integer.format(player.minutes)} phút`}
          title="Chọn cầu thủ so sánh"
          description={`Tối thiểu 2 · tối đa 4 · 900 phút trở lên · ${players.length} cầu thủ đủ điều kiện`}
          placeholder="Tìm theo tên cầu thủ hoặc câu lạc bộ…"
        />
      </div>
    );
  }
  if (activeTab === "player-contribution" || activeTab === "matchday-performance" || activeTab === "points-progression" || activeTab === "head-to-head") {
    if (activeTab === "points-progression" || activeTab === "head-to-head") {
      return (
        <div className="control-bar progression-controls">
          {clubControl}
          <p>{activeTab === "points-progression"
            ? "So sánh điểm cộng dồn của CLB qua tối đa 3 mùa có dữ liệu."
            : "Xem kết quả với từng đối thủ, gộp tối đa 3 mùa có dữ liệu."}</p>
        </div>
      );
    }
    return <div className="control-bar">{clubControl}</div>;
  }
  if (activeTab === "squad-value") {
    return (
      <div className="control-bar">
        <SelectField label="Câu lạc bộ" value={clubId} onChange={setClubId} disabled={!teams.length}>
          <option value="">Toàn giải</option>
          {teams.map((team) => <option key={team.club_id} value={team.club_id}>{team.club_name}</option>)}
        </SelectField>
      </div>
    );
  }
  if (activeTab === "team-strength") {
    return (
      <div className="control-bar">
        <SearchableMultiPicker
          items={teams}
          selectedIds={strengthClubs}
          onChange={setStrengthClubs}
          getId={(team) => team.club_id}
          getLabel={(team) => team.club_name}
          getSearchText={(team) => team.club_name}
          maxSelections={4}
          title="Chọn câu lạc bộ"
          description="Tối đa 4 CLB · bỏ chọn hết để xem toàn giải"
          placeholder="Tìm câu lạc bộ…"
        />
      </div>
    );
  }
  return <div className="control-bar control-note"><span>{selectedTab?.mode === "static" ? "Ảnh báo cáo đã xuất và đối chiếu cùng bộ dữ liệu đang phục vụ." : ""}</span></div>;
}

function PlayerRadar({ result }) {
  const data = result?.data || [];
  const axes = result?.meta?.axes || [];
  if (!data.length || !axes.length) return <EmptyState text="Hãy chọn từ 2 đến 4 cầu thủ đủ điều kiện." />;
  const option = {
    ...baseChart(),
    grid: { left: 24, right: 24, top: 30, bottom: 28, containLabel: true },
    legend: { bottom: 0, type: "scroll" },
    tooltip: {
      trigger: "item",
      formatter: (item) => {
        const row = data[item.dataIndex];
        if (!row) return item.name;
        return "<strong>" + row.player_name + " · " + row.club_name + "</strong><br/>" +
          axes.map((axis) => axis.label + ": " + (row[axis.key] == null ? "thiếu dữ liệu" : Number(row[axis.key]).toFixed(2)) +
            " · " + (row["pct_" + axis.key] == null ? "—" : Number(row["pct_" + axis.key]).toFixed(0) + " percentile")).join("<br/>");
      }
    },
    radar: {
      center: ["50%", "48%"],
      radius: "68%",
      indicator: axes.map((axis) => ({ name: axis.label, max: 100 })),
      axisName: { color: "#526176", fontSize: 11 },
      splitArea: { areaStyle: { color: ["#fbfcfc", "#f2f6f4"] } },
      splitLine: { lineStyle: { color: "#dfe7e4" } },
      axisLine: { lineStyle: { color: "#dfe7e4" } }
    },
    series: [{
      type: "radar",
      data: data.map((row, index) => {
        const color = PLAYER_RADAR_COLORS[index % PLAYER_RADAR_COLORS.length];
        return {
          name: row.player_name + " · " + row.club_name,
          value: axes.map((axis) => row["pct_" + axis.key]),
          itemStyle: { color },
          lineStyle: { color, width: 2 },
          areaStyle: { color, opacity: 0.1 }
        };
      })
    }]
  };
  return (
    <div className="chart-wrap chart-radar">
      <ChartPanel option={option} style={{ height: 500, width: "100%" }} notMerge lazyUpdate aria-label="Radar so sánh percentile cầu thủ" />
    </div>
  );
}

function Contribution({ result }) {
  const rows = result?.data || [];
  if (!rows.length) return <EmptyState text="Không có thống kê cho câu lạc bộ này." />;
  const chartFor = (metric, title, color) => {
    const top = [...rows].sort((a, b) => (b[metric] ?? -1) - (a[metric] ?? -1)).slice(0, 12).reverse();
    const option = {
      ...baseChart(),
      title: { text: title, left: 12, top: 4, textStyle: { color: "#17243b", fontSize: 14, fontWeight: 700 } },
      grid: { left: 56, right: 22, top: 42, bottom: 20, containLabel: true },
      tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
      xAxis: { type: "value", axisLabel: { color: "#778397" }, splitLine: { lineStyle: { color: "#edf0f2" } } },
      yAxis: { type: "category", data: top.map((row) => row.player_name), axisLabel: { color: "#526176", width: 112, overflow: "truncate" }, axisLine: { show: false }, axisTick: { show: false } },
      series: [{
        type: "bar",
        data: top.map((row) => row[metric]),
        barMaxWidth: 20,
        itemStyle: { color, borderRadius: [0, 5, 5, 0] }
      }]
    };
    return <ChartPanel key={metric} option={option} style={{ height: 390, width: "100%" }} notMerge lazyUpdate />;
  };
  return (
    <div className="dual-chart">
      <div className="chart-wrap">{chartFor("minutes", "Thời lượng thi đấu · phút", "#247a5a")}</div>
      <div className="chart-wrap">{chartFor("goal_contributions", "Bàn thắng + kiến tạo", "#e83267")}</div>
    </div>
  );
}

function SquadValue({ result }) {
  const rows = result?.data || [];
  const available = rows.filter((row) => row.market_value_eur != null).slice(0, 14).reverse();
  if (!rows.length) return <EmptyState text="Chưa có cầu thủ trong lựa chọn này." />;
  const chart = {
    ...baseChart(),
    grid: { left: 148, right: 70, top: 28, bottom: 34, containLabel: false },
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" }, formatter: (items) => {
      const item = items[0];
      const row = available[item.dataIndex];
      return row ? "<strong>" + row.player_name + "</strong><br/>" + row.club_name + "<br/>" + formatEur(row.market_value_eur) : "";
    } },
    xAxis: { type: "value", name: "Triệu EUR", axisLabel: { color: "#778397" }, splitLine: { lineStyle: { color: "#edf0f2" } } },
    yAxis: { type: "category", data: available.map((row) => row.player_name), axisLabel: { color: "#526176", width: 130, overflow: "truncate" }, axisLine: { show: false }, axisTick: { show: false } },
    series: [{
      type: "bar",
      barMaxWidth: 18,
      data: available.map((row) => ({
        value: row.market_value_eur / 1_000_000,
        itemStyle: { color: ({ GK: "#247a5a", DF: "#5a8c83", MF: "#6485b0", FW: "#e83267" })[row.position] || "#8292a8", borderRadius: [0, 4, 4, 0] }
      })),
      label: { show: true, position: "right", color: "#526176", fontSize: 10, formatter: (item) => item.value.toLocaleString("vi-VN", { maximumFractionDigits: 1 }) }
    }]
  };
  const groups = result.meta?.positions || [];
  return (
    <>
      <div className="chart-wrap"><ChartPanel option={chart} style={{ height: Math.max(400, available.length * 31), width: "100%" }} notMerge lazyUpdate aria-label="Giá trị thị trường cầu thủ theo vị trí" /></div>
      <div className="coverage-strip">
        {groups.map((group) => (
          <span key={group.position}><b>{POSITIONS[group.position] || group.position}</b> · {integer.format(group.valued_rows)}/{integer.format(group.player_rows)} có định giá</span>
        ))}
      </div>
      <details className="data-table">
        <summary>Xem danh sách cầu thủ và giá trị còn thiếu</summary>
        <div className="table-scroll">
          <table>
            <thead><tr><th>Cầu thủ</th><th>Vị trí</th><th>Định giá gần nhất</th><th>Ngày định giá</th></tr></thead>
            <tbody>{rows.map((row) => <tr key={row.player_id + row.club_id}>
              <td>{row.player_name}</td><td>{POSITIONS[row.position] || row.position}</td>
              <td>{formatEur(row.market_value_eur)}</td><td>{row.valuation_date || "—"}</td>
            </tr>)}</tbody>
          </table>
        </div>
      </details>
    </>
  );
}

function Matchday({ result }) {
  const rows = result?.data || [];
  if (!rows.length) return <EmptyState text="Không nối được lịch vòng cho câu lạc bộ này." />;
  const option = {
    ...baseChart(),
    legend: { top: 4, right: 20, data: ["Bàn thắng", "Bàn thua", "Điểm cộng dồn"] },
    grid: { left: 54, right: 56, top: 52, bottom: 55, containLabel: true },
    xAxis: { type: "category", name: "Vòng", data: rows.map((row) => row.matchday), axisLabel: { color: "#778397" }, axisLine: { lineStyle: { color: "#dfe5e9" } } },
    yAxis: [
      { type: "value", name: "Bàn / vòng", minInterval: 1, axisLabel: { color: "#778397" }, splitLine: { lineStyle: { color: "#edf0f2" } } },
      { type: "value", name: "Điểm cộng dồn", minInterval: 1, axisLabel: { color: "#778397" }, splitLine: { show: false } }
    ],
    series: [
      { name: "Bàn thắng", type: "bar", data: rows.map((row) => row.goals_for), itemStyle: { color: "#247a5a", borderRadius: [4, 4, 0, 0] }, barMaxWidth: 16 },
      { name: "Bàn thua", type: "bar", data: rows.map((row) => row.goals_against), itemStyle: { color: "#e83267", borderRadius: [4, 4, 0, 0] }, barMaxWidth: 16 },
      { name: "Điểm cộng dồn", type: "line", yAxisIndex: 1, data: rows.map((row) => row.cumulative_points), step: "end", symbol: "none", lineStyle: { color: "#4f77a8", width: 3 } }
    ]
  };
  return <div className="chart-wrap"><ChartPanel option={option} style={{ height: 465, width: "100%" }} notMerge lazyUpdate aria-label="Bàn thắng, bàn thua, điểm theo vòng" /></div>;
}

function HeadToHead({ result }) {
  const [query, setQuery] = useState("");
  const [sortBy, setSortBy] = useState("record");
  const allRows = result?.data || [];
  const normalizedQuery = normalizeSearch(query);
  const rows = allRows
    .filter((row) => !normalizedQuery || normalizeSearch(row.opponent_name).includes(normalizedQuery))
    .sort((left, right) => {
      if (sortBy === "wins") return right.wins - left.wins || left.losses - right.losses || left.opponent_name.localeCompare(right.opponent_name);
      if (sortBy === "losses") return right.losses - left.losses || left.wins - right.wins || left.opponent_name.localeCompare(right.opponent_name);
      if (sortBy === "name") return left.opponent_name.localeCompare(right.opponent_name);
      return (right.wins - right.losses) - (left.wins - left.losses) || right.matches - left.matches;
    });
  if (!allRows.length) return <EmptyState text="Chưa có dữ liệu đối đầu trong các mùa đã chọn." />;

  const mostWins = [...allRows].sort((left, right) => right.wins - left.wins || left.losses - right.losses)[0];
  const mostLosses = [...allRows].sort((left, right) => right.losses - left.losses || left.wins - right.wins)[0];
  const option = {
    ...baseChart(),
    legend: { top: 2, right: 18, data: ["Thắng", "Hòa", "Thua"] },
    grid: { left: 126, right: 28, top: 38, bottom: 48, containLabel: true },
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
    xAxis: { type: "value", name: "Số trận", min: 0, minInterval: 1, axisLabel: { color: "#778397" }, splitLine: { lineStyle: { color: "#edf0f2" } } },
    yAxis: { type: "category", inverse: true, data: rows.map((row) => row.opponent_name), axisLabel: { color: "#526176", width: 112, overflow: "truncate" }, axisLine: { show: false }, axisTick: { show: false } },
    series: [
      { name: "Thắng", type: "bar", stack: "results", data: rows.map((row) => row.wins), itemStyle: { color: "#247a5a" }, barMaxWidth: 20 },
      { name: "Hòa", type: "bar", stack: "results", data: rows.map((row) => row.draws), itemStyle: { color: "#e5ad45" }, barMaxWidth: 20 },
      { name: "Thua", type: "bar", stack: "results", data: rows.map((row) => row.losses), itemStyle: { color: "#e83267" }, barMaxWidth: 20 }
    ]
  };

  return (
    <>
      <div className="h2h-summary">
        <div className="h2h-stat"><span>THẮNG</span><strong>{integer.format(result.meta?.wins || 0)}</strong></div>
        <div className="h2h-stat"><span>HÒA</span><strong>{integer.format(result.meta?.draws || 0)}</strong></div>
        <div className="h2h-stat"><span>THUA</span><strong>{integer.format(result.meta?.losses || 0)}</strong></div>
        <div className="h2h-stat h2h-featured"><span>THẮNG NHIỀU NHẤT TRƯỚC</span><strong>{mostWins?.opponent_name || "—"}</strong><small>{mostWins ? `${mostWins.wins} thắng · ${mostWins.matches} trận` : ""}</small></div>
        <div className="h2h-stat h2h-featured"><span>THUA NHIỀU NHẤT TRƯỚC</span><strong>{mostLosses?.opponent_name || "—"}</strong><small>{mostLosses ? `${mostLosses.losses} thua · ${mostLosses.matches} trận` : ""}</small></div>
      </div>
      <div className="h2h-toolbar">
        <label className="picker-search">
          <Search size={15} aria-hidden="true" />
          <input type="search" value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Tìm đối thủ…" aria-label="Tìm đối thủ" />
          {query && <button type="button" onClick={() => setQuery("")} aria-label="Xóa từ khóa tìm đối thủ"><X size={14} /></button>}
        </label>
        <label className="h2h-sort"><span>Sắp xếp</span>
          <select value={sortBy} onChange={(event) => setSortBy(event.target.value)}>
            <option value="record">Hiệu số thắng – thua</option>
            <option value="wins">Thắng nhiều nhất</option>
            <option value="losses">Thua nhiều nhất</option>
            <option value="name">Tên đối thủ</option>
          </select>
        </label>
        <span className="h2h-result-count">{rows.length}/{allRows.length} đối thủ</span>
      </div>
      {rows.length ? (
        <div className="chart-wrap">
          <ChartPanel option={option} style={{ height: Math.max(450, rows.length * 30 + 100), width: "100%" }} notMerge lazyUpdate aria-label={`Thắng hòa thua của ${result.meta?.club_name} với các đối thủ trong 3 mùa`} />
          <p className="chart-caption">Mỗi thanh gộp các trận của CLB được chọn với một đối thủ qua các mùa so sánh.</p>
        </div>
      ) : <EmptyState text="Không tìm thấy đối thủ phù hợp." />}
      <details className="data-table">
        <summary>Xem số trận thắng – hòa – thua theo từng đối thủ</summary>
        <div className="table-scroll">
          <table>
            <thead><tr><th>Đối thủ</th><th>Thắng</th><th>Hòa</th><th>Thua</th><th>Số trận</th><th>Số mùa</th></tr></thead>
            <tbody>{rows.map((row) => <tr key={row.opponent_club_id}>
              <td>{row.opponent_name}</td><td>{row.wins}</td><td>{row.draws}</td><td>{row.losses}</td><td>{row.matches}</td><td>{row.seasons_played}</td>
            </tr>)}</tbody>
          </table>
        </div>
      </details>
    </>
  );
}

function PointsProgression({ result }) {
  const rows = result?.data || [];
  if (!rows.length) return <EmptyState text="Chưa có vòng đấu được nối cho lựa chọn này." />;
  const seasonTotals = [...(result.meta?.season_totals || [])].sort((left, right) => left.season.localeCompare(right.season));
  const seasons = [...new Set(rows.map((row) => row.season))].sort((left, right) => left.localeCompare(right));
  const roundNumbers = Array.from({ length: 38 }, (_, index) => index + 1);
  const seasonRows = new Map(seasons.map((season) => [
    season,
    new Map(rows.filter((row) => row.season === season).map((row) => [Number(row.matchday), row]))
  ]));
  const progressionColors = ["#9b7896", "#637f9e", "#48206a"];
  const option = {
    ...baseChart(),
    legend: { top: 4, right: 16, data: seasons.map(shortSeason) },
    grid: { left: 58, right: 30, top: 48, bottom: 52, containLabel: true },
    tooltip: { trigger: "axis", formatter: (items) => {
      const matchday = Number(items[0]?.axisValue);
      const lines = items.map((item) => {
        const row = seasonRows.get(seasons[item.seriesIndex])?.get(matchday);
        return row
          ? `${item.marker}${item.seriesName}: <b>${row.cumulative_points} điểm</b> · vòng này +${row.earned_points}`
          : `${item.marker}${item.seriesName}: chưa có trận nối được`;
      });
      return [`Vòng ${matchday}`, ...lines].join("<br/>");
    } },
    xAxis: { type: "category", name: "Vòng", boundaryGap: false, data: roundNumbers, axisLabel: { color: "#778397" }, axisLine: { lineStyle: { color: "#dfe5e9" } } },
    yAxis: { type: "value", name: "Điểm cộng dồn", min: 0, minInterval: 1, axisLabel: { color: "#778397" }, splitLine: { lineStyle: { color: "#edf0f2" } } },
    series: seasons.map((season, index) => {
      const color = progressionColors[index % progressionColors.length];
      const pointsByRound = seasonRows.get(season);
      return {
        type: "line",
        name: shortSeason(season),
        step: "end",
        connectNulls: false,
        data: roundNumbers.map((round) => pointsByRound.get(round)?.cumulative_points ?? null),
        symbol: "circle",
        showSymbol: false,
        symbolSize: 5,
        lineStyle: { color, width: index === seasons.length - 1 ? 3 : 1.8, opacity: index === seasons.length - 1 ? 1 : 0.72 },
        itemStyle: { color },
        emphasis: { focus: "series", lineStyle: { width: 3 } }
      };
    })
  };
  return (
    <>
      <div className="chart-wrap">
        <ChartPanel option={option} style={{ height: 475, width: "100%" }} notMerge lazyUpdate aria-label={`So sánh điểm cộng dồn 3 mùa của ${result.meta?.club_name || "câu lạc bộ"}`} />
        <p className="chart-caption">Mỗi đường là một mùa, trục ngang là vòng 1–38; tính 3 điểm/trận thắng, 1 điểm/trận hòa. Các mùa được tính độc lập.</p>
      </div>
      <div className="progression-summary" aria-label="Tổng điểm theo mùa">
        {seasonTotals.map((summary, index) => (
          <div className="progression-season" key={summary.season} style={{ "--season-color": progressionColors[index % progressionColors.length] }}>
            <span>{shortSeason(summary.season)}</span>
            <strong>{integer.format(summary.points_from_rounds)}<small> điểm</small></strong>
            <small>{integer.format(summary.matches)} trận được nối</small>
            {summary.difference != null && summary.difference !== 0 && (
              <small className="progression-difference">Chênh {summary.difference > 0 ? "+" : ""}{integer.format(summary.difference)} với Silver</small>
            )}
          </div>
        ))}
      </div>
    </>
  );
}

function TeamStrength({ result }) {
  const rows = result?.data || [];
  const axes = result?.meta?.axes || [];
  if (!rows.length) return <EmptyState text="Chọn tối đa bốn CLB hoặc xóa lựa chọn để xem toàn giải." />;
  const seriesData = rows.map((row) => ({
    name: row.club_name,
    value: axes.map((axis) => row[axis.key]),
    lineStyle: { width: 2 },
    areaStyle: { opacity: 0.08 }
  }));
  const option = {
    ...baseChart(),
    grid: { left: 24, right: 24, top: 30, bottom: 22, containLabel: true },
    legend: { bottom: 0, type: "scroll" },
    tooltip: { trigger: "item" },
    radar: {
      center: ["50%", "47%"],
      radius: "67%",
      indicator: axes.map((axis) => ({ name: axis.label, max: 100 })),
      axisName: { color: "#526176", fontSize: 11 },
      splitArea: { areaStyle: { color: ["#fbfcfc", "#f2f6f4"] } },
      splitLine: { lineStyle: { color: "#dfe7e4" } },
      axisLine: { lineStyle: { color: "#dfe7e4" } }
    },
    series: [{ type: "radar", data: seriesData }]
  };
  return (
    <div className="chart-wrap chart-radar">
      <ChartPanel option={option} style={{ height: 500, width: "100%" }} notMerge lazyUpdate aria-label="Radar percentile sức mạnh các đội" />
      <p className="chart-caption">Trục chất lượng đội hình có thể để trống khi dữ liệu định giá không đủ; không thay giá trị thiếu bằng 0.</p>
    </div>
  );
}

function StaticReport({ result }) {
  if (!result) return <EmptyState text="Báo cáo tĩnh chưa có dữ liệu." />;
  return (
    <div className={"static-figure " + (result.image_url?.includes("head-to-head") ? "static-figure-tall" : "")}>
      <img src={apiUrl(result.image_url)} alt={result.title || "Biểu đồ phân tích"} />
      <p className="chart-caption">{result.title} · {result.meta?.season || ""}</p>
    </div>
  );
}

function EmptyState({ text }) {
  return <div className="empty-state"><Info size={19} /><span>{text}</span></div>;
}

const DYNAMIC_ENDPOINTS = {
  "head-to-head": "/api/head-to-head",
  "player-radar": "/api/player-radar",
  "player-contribution": "/api/player-contribution",
  "squad-value": "/api/squad-value",
  "matchday-performance": "/api/matchday-performance",
  "points-progression": "/api/points-progression",
  "team-strength": "/api/team-strength"
};

function App() {
  const [season, setSeason] = useState("2024-2025");
  const [activeTab, setActiveTab] = useState("attack-defense");
  const [position, setPosition] = useState("FW");
  const [clubId, setClubId] = useState("");
  const [radarPlayers, setRadarPlayers] = useState([]);
  const [strengthClubs, setStrengthClubs] = useState([]);
  const [metadata, setMetadata] = useState(null);
  const [result, setResult] = useState(null);
  const [metadataError, setMetadataError] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const [apiStatus, setApiStatus] = useState("checking");
  const [retryKey, setRetryKey] = useState(0);

  useEffect(() => {
    const controller = new AbortController();
    setMetadataError("");
    getJson("/api/metadata", { season, position, min_minutes: 900 }, controller.signal)
      .then((data) => {
        setMetadata(data);
        setApiStatus("ready");
      })
      .catch((requestError) => {
        if (requestError.name === "AbortError") return;
        setMetadataError(requestError.message);
        setApiStatus("offline");
      });
    return () => controller.abort();
  }, [season, position, retryKey]);

  useEffect(() => {
    if (!metadata) return;
    const teamIds = new Set(metadata.teams.map((team) => team.club_id));
    setClubId((current) => teamIds.has(current) ? current : (teamIds.has("ARS") ? "ARS" : metadata.teams[0]?.club_id || ""));
    setStrengthClubs((current) => {
      const existing = current.filter((id) => teamIds.has(id));
      return existing.length ? existing.slice(0, 4) : ["ARS", "MCI", "LIV", "CHE"].filter((id) => teamIds.has(id));
    });
    const playerIds = new Set(metadata.players.map((player) => player.selection_id));
    setRadarPlayers((current) => {
      const existing = current.filter((id) => playerIds.has(id)).slice(0, 4);
      if (existing.length >= 2) return existing;
      const additions = metadata.players
        .filter((player) => !existing.includes(player.selection_id))
        .sort((a, b) => b.minutes - a.minutes || a.selection_id.localeCompare(b.selection_id))
        .slice(0, 2 - existing.length)
        .map((player) => player.selection_id);
      return existing.concat(additions);
    });
  }, [metadata]);

  useEffect(() => {
    const controller = new AbortController();
    const tab = TABS.find((item) => item.id === activeTab);
    if (!tab) return () => controller.abort();
    setError("");
    setResult(null);
    setLoading(true);
    const fail = (requestError) => {
      if (requestError.name === "AbortError") return;
      setError(requestError.message);
      setLoading(false);
      setApiStatus("offline");
    };

    if (tab.mode === "static") {
      getJson("/api/reports/" + activeTab, { season }, controller.signal)
        .then((data) => {
          setResult(data);
          setLoading(false);
          setApiStatus("ready");
        })
        .catch(fail);
      return () => controller.abort();
    }
    if (!metadata) return () => controller.abort();
    if (activeTab === "player-radar") {
      const eligible = new Set(metadata.players.map((player) => player.selection_id));
      if (radarPlayers.length < 2 || radarPlayers.some((id) => !eligible.has(id))) {
        setLoading(false);
        return () => controller.abort();
      }
      getJson(DYNAMIC_ENDPOINTS[activeTab], { season, position, players: radarPlayers, min_minutes: 900 }, controller.signal)
        .then((data) => {
          setResult(data);
          setLoading(false);
          setApiStatus("ready");
        })
        .catch(fail);
    } else if (activeTab === "team-strength") {
      getJson(DYNAMIC_ENDPOINTS[activeTab], { season, clubs: strengthClubs }, controller.signal)
        .then((data) => {
          setResult(data);
          setLoading(false);
          setApiStatus("ready");
        })
        .catch(fail);
    } else if (activeTab === "points-progression" || activeTab === "head-to-head") {
      if (!clubId || !metadata.teams.some((team) => team.club_id === clubId)) {
        setLoading(false);
        return () => controller.abort();
      }
      getJson(DYNAMIC_ENDPOINTS[activeTab], { club_id: clubId, seasons: metadata.seasons || [] }, controller.signal)
        .then((data) => {
          setResult(data);
          setLoading(false);
          setApiStatus("ready");
        })
        .catch(fail);
    } else {
      if (activeTab !== "squad-value" && (!clubId || !metadata.teams.some((team) => team.club_id === clubId))) {
        setLoading(false);
        return () => controller.abort();
      }
      const params = { season, club_id: clubId };
      if (activeTab === "squad-value" && !clubId) delete params.club_id;
      getJson(DYNAMIC_ENDPOINTS[activeTab], params, controller.signal)
        .then((data) => {
          setResult(data);
          setLoading(false);
          setApiStatus("ready");
        })
        .catch(fail);
    }
    return () => controller.abort();
  }, [activeTab, season, position, clubId, radarPlayers, strengthClubs, metadata, retryKey]);

  useEffect(() => {
    const controller = new AbortController();
    getHealth(controller.signal)
      .then((health) => setApiStatus(health.database && health.status === "ready" ? "ready" : "checking"))
      .catch(() => setApiStatus("offline"));
    return () => controller.abort();
  }, [retryKey]);

  const currentTab = TABS.find((tab) => tab.id === activeTab) || TABS[0];
  const currentTitle = useMemo(() => ({
    "player-radar": "So sánh cầu thủ cùng vị trí",
    "player-contribution": "Thời lượng và đóng góp trực tiếp",
    "squad-value": "Giá trị thị trường đội hình",
    "age-distribution": "Cơ cấu tuổi cầu thủ",
    "head-to-head": "Đối đầu của một CLB qua 3 mùa",
    "matchday-performance": "Phong độ theo từng vòng",
    "attack-defense": "Hiệu quả tấn công và phòng ngự",
    "points-progression": "Điểm cộng dồn qua 3 mùa",
    "team-strength": "Năm trục sức mạnh đội bóng",
    "spending-vs-achievement": "Chi tiêu chuyển nhượng và thứ hạng"
  })[activeTab], [activeTab]);

  const retry = () => {
    setResult(null);
    setRetryKey((key) => key + 1);
  };

  const content = () => {
    if (loading) return <LoadingState />;
    if (error) return <ErrorState message={error} onRetry={retry} />;
    if (!result) {
      if (metadataError) return <ErrorState message={metadataError} onRetry={retry} />;
      if (activeTab === "player-radar" && (metadata?.players?.length || 0) < 2) {
        return <EmptyState text="Mùa hoặc vị trí này không có đủ hai cầu thủ đạt ngưỡng 900 phút." />;
      }
      return <EmptyState text="Chọn điều kiện để tải báo cáo." />;
    }
    if (activeTab === "head-to-head") return <HeadToHead result={result} />;
    if (currentTab.mode === "static") return <StaticReport result={result} />;
    if (activeTab === "player-radar") return <PlayerRadar result={result} />;
    if (activeTab === "player-contribution") return <Contribution result={result} />;
    if (activeTab === "squad-value") return <SquadValue result={result} />;
    if (activeTab === "matchday-performance") return <Matchday result={result} />;
    if (activeTab === "points-progression") return <PointsProgression result={result} />;
    if (activeTab === "team-strength") return <TeamStrength result={result} />;
    return <EmptyState text="Chưa có biểu đồ cho tab này." />;
  };

  return (
    <main className="app-shell">
      <header className="topbar">
        <a className="brand" href="/" aria-label="Premier League Analytics">
          <span className="brand-pitch"><i /><i /><i /></span>
          <span className="brand-name">PL<span> / </span>LAB</span>
        </a>
        <div className="topbar-right">
          <span className="dataset-note">BÁO CÁO ĐỒ ÁN · 3 MÙA</span>
          <label className="season-select">
            <span>{activeTab === "points-progression" || activeTab === "head-to-head" ? "MÙA CHỌN CLB" : "MÙA GIẢI"}</span>
            <select data-testid="season" value={season} onChange={(event) => setSeason(event.target.value)}>
              {(metadata?.seasons || ["2024-2025", "2023-2024", "2022-2023"]).map((value) => (
                <option value={value} key={value}>{shortSeason(value)}</option>
              ))}
            </select>
          </label>
        </div>
      </header>

      <section className="hero">
        <div className="hero-copy">
          <div className="eyebrow"><span className="live-dot" /> PHÒNG PHÂN TÍCH · PREMIER LEAGUE</div>
          <h1>Trận đấu để lại <em>dấu vết.</em></h1>
          <p>Mười góc nhìn từ dữ liệu Silver đã chuẩn hóa. Mỗi kết luận đi cùng cách tính, cỡ mẫu và giới hạn diễn giải.</p>
        </div>
        <div className="pitch-stamp" aria-hidden="true">
          <div className="pitch-outline"><span className="pitch-midline" /><span className="pitch-circle" /><span className="pitch-dot" /></div>
          <span>SEASON<br />LEDGER</span>
        </div>
      </section>

      <section className="status-strip" aria-live="polite">
        <span className={"status-light " + (apiStatus === "ready" ? "is-ready" : apiStatus === "offline" ? "is-offline" : "")} />
        <span>{apiStatus === "ready" ? "PostgreSQL sẵn sàng" : apiStatus === "offline" ? "API chưa kết nối" : "Đang kết nối PostgreSQL"}</span>
        <span className="status-divider">/</span>
        <span>Bộ dữ liệu: {metadata?.meta?.run_id || "đang kiểm tra"}</span>
        {apiStatus === "offline" && <button className="link-button" onClick={retry}>Kết nối lại</button>}
      </section>

      <nav className="tabs" aria-label="Mười phân tích">
        {TABS.map((tab) => {
          const Icon = tab.icon;
          return (
            <button
              className={"tab-button " + (activeTab === tab.id ? "is-active" : "")}
              key={tab.id}
              data-testid={"tab-" + tab.id}
              onClick={() => setActiveTab(tab.id)}
              aria-current={activeTab === tab.id ? "page" : undefined}
            >
              <Icon size={15} strokeWidth={1.8} />
              <span>{tab.label}</span>
            </button>
          );
        })}
      </nav>

      <section className="report-section">
        <div className="report-heading">
          <div>
            <div className="eyebrow">{currentTab.mode === "static" ? "BÁO CÁO THEO MÙA" : "TRUY VẤN TƯƠNG TÁC"}</div>
            <h2>{currentTitle}</h2>
          </div>
          <div className="season-stamp">
            {activeTab === "points-progression" || activeTab === "head-to-head"
              ? `So sánh ${metadata?.seasons?.length || 3} mùa`
              : <>{shortSeason(season)} <span>·</span> Premier League</>}
          </div>
        </div>

        <ControlBar
          activeTab={activeTab}
          metadata={metadata}
          season={season}
          position={position}
          setPosition={setPosition}
          clubId={clubId}
          setClubId={setClubId}
          radarPlayers={radarPlayers}
          setRadarPlayers={setRadarPlayers}
          strengthClubs={strengthClubs}
          setStrengthClubs={setStrengthClubs}
        />

        {metadataError && !error && <ErrorState message={metadataError} onRetry={retry} />}
        <div className="report-card" data-testid="report" data-analysis={activeTab} data-run-id={result?.meta?.run_id || ""} data-source={result?.meta?.source || ""} data-ready={Boolean(result && !loading && !error)}>
          {content()}
          {result && !error && <Insight value={result.insight} />}
          <footer className="source-line">
            <span>NGUỒN <b>Silver đã nghiệm thu</b></span>
            <span>TRUY VẤN <b>PostgreSQL</b></span>
            <span>GIAO DIỆN <b>HuTube PL Lab</b></span>
          </footer>
        </div>
      </section>

      <footer className="page-footer">
        <span>Premier League Analysis</span>
        <span>Thống kê mô tả không thay thế kết luận nhân quả.</span>
      </footer>
    </main>
  );
}

export default App;
