"""Danh valuation/trajectory and percentile cosine similarity, with saved inputs."""
import math

METRICS = {
    'FW': ['goals_p90', 'sot_p90', 'assists_p90', 'crosses_p90', 'fouls_dr_p90', 'tackles_p90', 'minutes'],
    'MF': ['assists_p90', 'tackles_p90', 'int_p90', 'goals_p90', 'fouls_dr_p90', 'crosses_p90', 'minutes'],
    'DF': ['int_p90', 'tackles_p90', 'fouls_dr_p90', 'crosses_p90', 'goals_p90', 'minutes'],
    'GK': ['goals_p90', 'tackles_p90', 'fouls_dr_p90', 'minutes'],
}


def js_round(value, digits=0):
    scale = 10 ** digits
    return math.floor(value * scale + 0.5) / scale


def valuation(age, position, market_value_mil, goals_assists, minutes, tier):
    position_coeff = {'FW': 1.20, 'MF': 1.05, 'DF': .90, 'GK': .75}[position]
    age_coeff = 1.22 if age <= 20 else 1.15 if age <= 23 else 1.08 if age <= 26 else 1 if age <= 28 else .82 if age <= 30 else .58
    perf = min(1.45, max(.65, (goals_assists * 1.5 + minutes / 3420 * 20) / 35))
    tier_coeff = {'tier1': 1.25, 'tier2': 1, 'tier3': .85}[tier]
    fee = js_round(market_value_mil * .45 + market_value_mil * position_coeff * age_coeff * perf * tier_coeff * .55, 1)
    factors = [1.10, 1.18, 1.20] if age <= 22 else [1.05, 1.04, .96] if age <= 25 else [.95, .85, .72] if age <= 28 else [.80, .62, .45]
    risk = 'cao' if age >= 29 and fee > 50 else 'trung bình' if age >= 27 or fee > 90 else 'thấp'
    return {'estimated_fee_mil_eur': fee, 'lower_mil_eur': js_round(fee * .92, 1), 'upper_mil_eur': js_round(fee * 1.08, 1),
            'risk': risk, 'trajectory': [{'year': i + 1, 'age': age + i + 1, 'value_mil_eur': js_round(fee * factor, 1)} for i, factor in enumerate(factors)],
            'is_simulated': True, 'method': 'Danh valuation heuristic v1', 'note': 'Mô phỏng theo công thức; không phải giá chuyển nhượng hoặc dự báo chính thức.'}


def similar(players, player_id, candidate_season=None, limit=6):
    current = next((p for p in players if p['id'] == player_id), None)
    if current is None:
        raise ValueError('Không tìm thấy cầu thủ')
    metrics = METRICS.get(current['pos'], METRICS['MF'])
    ranked = []
    for candidate in players:
        if candidate.get('source_id', candidate['id']) == current.get('source_id', current['id']) or candidate['pos'] != current['pos'] or (candidate_season and candidate['season'] != candidate_season):
            continue
        # Match original JS `value || 50`, including its treatment of zero percentiles.
        a = [current['pct'].get(m) or 50 for m in metrics]
        b = [candidate['pct'].get(m) or 50 for m in metrics]
        score = sum(x * y for x, y in zip(a, b)) / math.sqrt(sum(x*x for x in a) * sum(y*y for y in b)) * 100
        ranked.append({'player': candidate, 'similarity': js_round(score, 1), 'saved_mil_eur': js_round(current['val'] - candidate['val'], 1)})
    ranked.sort(key=lambda row: -row['similarity'])
    return {'player': current, 'metrics': metrics, 'recommendations': ranked[:limit], 'method': 'Danh percentile cosine similarity v1'}
