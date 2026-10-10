"""As-of forecasting and score-state inference. No filesystem or DB at inference."""
from datetime import date, timedelta
import math
import numpy as np
from .schema import LABELS, METRICS, validate_stats

STYLE = ('possession', 'shots', 'shots_on_target', 'shot_accuracy', 'crosses', 'interceptions', 'tackles_won', 'fouls_committed')
DEFAULT_STYLE = (50., 12., 4., 33.333, 15., 8., 8., 10.)
TEAM_FEATURES = ('attack', 'conceded', 'form5_points', 'form10_goal_difference', 'venue_points', 'rest_days', *STYLE)
FEATURES = [s + '_' + k for s in ('home', 'away') for k in TEAM_FEATURES] + ['h2h_points_delta', 'h2h_goal_delta']


def history_before(rows, cutoff):
    """Exclude the entire target date (kickoff timestamps are unavailable)."""
    day = date.fromisoformat(str(cutoff))
    season = day.year if day.month >= 7 else day.year - 1
    return [r for r in rows if r['match_date'] < day.isoformat() and int(r['season'][:4]) >= season - 2]


def team_summary(history, club, venue, cutoff, half_life):
    games = []
    for r in history:
        if club not in (r['home_team'], r['away_team']):
            continue
        side = 'home' if r['home_team'] == club else 'away'
        gf, ga = r[side + '_goals'], r[('away' if side == 'home' else 'home') + '_goals']
        games.append((r, side, gf, ga, 3 if gf > ga else 1 if gf == ga else 0))
    games.sort(key=lambda g: (g[0]['match_date'], g[0]['match_id']))
    weights = np.array([2 ** (-(date.fromisoformat(cutoff) - date.fromisoformat(g[0]['match_date'])).days / half_life) for g in games])
    def mean(values, prior, strength=5., mask=None):
        pairs = [(float(v), float(w)) for v, w, enabled in zip(values, weights, mask if mask is not None else [True]*len(games)) if enabled and v is not None]
        return (sum(v*w for v,w in pairs) + strength*prior)/(sum(w for _,w in pairs)+strength)
    venue_mask = [g[1] == venue for g in games]
    style = [mean([g[0][g[1]+'_stats'].get(k) for g in games], prior) for k,prior in zip(STYLE, DEFAULT_STYLE)]
    last5, last10 = games[-5:], games[-10:]
    attack = mean([g[2] for g in games], 1.4)
    conceded = mean([g[3] for g in games], 1.4)
    # Small-sample shrinkage prevents one derby or a promoted club from dominating.
    form5 = (sum(g[4] for g in last5)+2*1.4)/(len(last5)+2)
    form10 = sum(g[2]-g[3] for g in last10)/(len(last10)+3)
    vp = mean([g[4] for g in games], 1.4, mask=venue_mask)
    rest = min(21, (date.fromisoformat(cutoff)-date.fromisoformat(games[-1][0]['match_date'])).days) if games else 7
    features = [attack, conceded, form5, form10, vp, rest, *style]
    return features, {'played':len(games), 'attack':attack, 'conceded':conceded, 'form5_points':form5, 'form10_goal_difference':form10,
                      'venue_points':vp, 'rest_days':rest, 'style':dict(zip(STYLE, style)), 'last_match':games[-1][0]['match_date'] if games else None}


def context(rows, home, away, cutoff, half_life=180):
    history = history_before(rows, cutoff)
    hf, hs = team_summary(history, home, 'home', cutoff, half_life)
    af, ass = team_summary(history, away, 'away', cutoff, half_life)
    h2h = []
    for r in history:
        if {r['home_team'],r['away_team']} != {home,away}:
            continue
        side = 'home' if r['home_team'] == home else 'away'
        g, a = r[side+'_goals'], r[('away' if side == 'home' else 'home')+'_goals']
        h2h.append({'match_id':r['match_id'], 'match_date':r['match_date'], 'season':r['season'],
                    'home_team':r['home_team'], 'away_team':r['away_team'], 'home_goals':r['home_goals'], 'away_goals':r['away_goals'],
                    'home_name':r.get('home_name',r['home_team']), 'away_name':r.get('away_name',r['away_team']),
                    'points_delta':3 if g>a else -3 if g<a else 0, 'goal_delta':g-a})
    h2h.sort(key=lambda r:(r['match_date'],r['match_id']),reverse=True)
    n = len(h2h)+6
    values = hf + af + [sum(r['points_delta'] for r in h2h)/n, sum(r['goal_delta'] for r in h2h)/n]
    return np.asarray(values), {'home':hs, 'away':ass, 'head_to_head':h2h, 'history_matches':len(history), 'cutoff_exclusive':cutoff}


def standard(values, model):
    return (np.asarray(values)-np.asarray(model['means']))/np.asarray(model['scales'])


def softmax(values):
    values = np.asarray(values)
    z = np.exp(values-values.max(axis=-1,keepdims=True))
    return z/z.sum(axis=-1,keepdims=True)


def rates(values, model):
    x = np.r_[1., standard(values, model)]
    return np.exp(np.clip(np.asarray(model['coefficients']) @ x, -4, 2.5))


def score_probabilities(lambdas, rho=0.):
    # Tail mass < 1e-10 at the maximum fitted lambda; no fixed 5-goal cutoff.
    def pmf(lam):
        p = [math.exp(-lam)]
        for k in range(1,81):
            p.append(p[-1]*lam/k)
            if k > lam and sum(p) > 1-1e-12:
                break
        return np.asarray(p)
    h,a = map(float,lambdas)
    matrix = np.outer(pmf(h),pmf(a))
    if rho:
        factors = [1-h*a*rho,1+h*rho,1+a*rho,1-rho]
        if min(factors) <= 0:
            raise ValueError('Invalid low-score correction')
        matrix[0,0]*=factors[0]; matrix[0,1]*=factors[1]; matrix[1,0]*=factors[2]; matrix[1,1]*=factors[3]
    matrix /= matrix.sum()
    return np.array([np.tril(matrix,-1).sum(),np.trace(matrix),np.triu(matrix,1).sum()])


def neighbors(query, refs, scaler, cutoff, k, half_life, mask=None):
    selected = [r for r in refs if r['match_date'] < cutoff and int(r['season'][:4]) >= (date.fromisoformat(cutoff).year if date.fromisoformat(cutoff).month>=7 else date.fromisoformat(cutoff).year-1)-2]
    if not selected:
        return np.array([.44,.26,.30]), []
    used = np.asarray(mask if mask is not None else [True]*len(query),dtype=bool)
    if not used.any():
        return np.array([.44,.26,.30]), []
    low,span = np.asarray(scaler['min'])[used],np.asarray(scaler['span'])[used]
    q = np.clip((np.asarray(query)[used]-low)/span,0,1)
    xs = np.clip((np.asarray([r['vector'] for r in selected])[:,used]-low)/span,0,1)
    denominator = np.linalg.norm(xs,axis=1)*np.linalg.norm(q)
    similarities = np.divide(xs@q,denominator,out=np.zeros(len(xs)),where=denominator>1e-12)
    # Tie order is deterministic. Zero vectors carry no invented similarity.
    order = sorted(range(len(selected)),key=lambda i:(-similarities[i],selected[i]['match_id']))[:min(k,len(selected))]
    counts = np.bincount([r['label'] for r in selected],minlength=3).astype(float)+1
    prior = counts/counts.sum()
    mass = 3*prior
    shown = []
    for i in order:
        r = selected[i]
        age = (date.fromisoformat(cutoff)-date.fromisoformat(r['match_date'])).days
        weight = max(0.,float(similarities[i]))**2 * 2**(-age/half_life)
        mass[r['label']] += weight
        shown.append({key:r[key] for key in ('match_id','match_date','season','home_team','away_team','home_name','away_name','home_goals','away_goals')} | {'similarity':float(similarities[i]),'weight':weight,'outcome':LABELS[r['label']]})
    return mass/mass.sum(), shown


def pregame(artifact, home, away, cutoff):
    values, details = context(artifact['history'],home,away,cutoff,artifact['half_life_days'])
    lam = rates(values,artifact['poisson'])
    base = score_probabilities(lam)
    near, shown = neighbors(values,artifact['references'],artifact['feature_scaler'],cutoff,artifact['k'],artifact['half_life_days'])
    alpha = artifact['neighbor_weight']
    p = softmax(((1-alpha)*np.log(np.clip(base,1e-12,1))+alpha*np.log(np.clip(near,1e-12,1)))/artifact['temperature'])
    return p, values, details, shown, lam


def clock(minute, stoppage=0):
    return (1 if minute<=45 else 2,minute,stoppage)


def score_at(events, home, minute, stoppage=0):
    state = clock(minute,stoppage)
    return tuple(sum(clock(int(g['minute']),int(g.get('stoppage_minutes') or 0))<=state and ((g['club_id']==home)==(side==0)) for g in events) for side in (0,1))


def live_vector(p, home_goals, away_goals, minute, stoppage=0):
    t = (minute + min(stoppage,15))/90
    margin = home_goals-away_goals
    # Explicit score/time interactions distinguish 1-0 at minute 10 and 85.
    return np.r_[np.log(np.clip(p,1e-8,1)), margin,abs(margin),home_goals+away_goals,t,t*t,margin*t,abs(margin)*t,margin*t*t]


def live_probabilities(artifact, p, home_goals, away_goals, minute, stoppage=0):
    model = artifact['live']
    x = np.r_[1.,standard(live_vector(p,home_goals,away_goals,minute,stoppage),model)]
    return softmax(np.asarray(model['coefficients'])@x/model['temperature'])


def validate_request(request):
    r = dict(request)
    r.setdefault('mode','pregame'); r.setdefault('home_goals',0); r.setdefault('away_goals',0)
    r.setdefault('minute',0); r.setdefault('stoppage_minutes',0); r.setdefault('finished',False)
    if r['mode'] not in ('pregame','live'):
        raise ValueError('Chế độ dự đoán không hợp lệ')
    if r['home_team']==r['away_team']:
        raise ValueError('Chọn hai CLB khác nhau')
    for k,upper in (('home_goals',30),('away_goals',30),('minute',90),('stoppage_minutes',30)):
        if isinstance(r[k],bool) or not isinstance(r[k],int) or not 0<=r[k]<=upper:
            raise ValueError(k+' ngoài khoảng hợp lệ')
    if r['stoppage_minutes'] and r['minute'] not in (45,90):
        raise ValueError('Bù giờ chỉ dùng tại phút 45 hoặc 90')
    for side in ('home','away'):
        r[side+'_stats'] = validate_stats(r.get(side+'_stats') or {},require=False)
    if r['mode']=='pregame' and (r['home_goals'] or r['away_goals'] or r['minute'] or r['stoppage_minutes'] or r['finished'] or any(r[s+'_stats'].get(k) is not None for s in ('home','away') for k in METRICS)):
        raise ValueError('Trước trận chỉ dùng lịch sử; hãy chuyển sang Trong trận để nhập tỉ số/thống kê')
    if r['mode']=='live' and not r['minute'] and (r['home_goals'] or r['away_goals'] or any(r[s+'_stats'].get(k) is not None for s in ('home','away') for k in METRICS)):
        raise ValueError('Nhập phút thi đấu lớn hơn 0 khi đã có bàn thắng hoặc thống kê')
    possessions = [r[s+'_stats']['possession'] for s in ('home','away')]
    if all(v is not None for v in possessions) and abs(sum(possessions)-100)>.5:
        raise ValueError('Kiểm soát bóng của hai đội phải có tổng 100%')
    return r


def forecast(request, artifact):
    r = validate_request(request)
    known = {m[s+'_team'] for m in artifact['history'] for s in ('home','away')}
    if r['home_team'] not in known or r['away_team'] not in known:
        raise ValueError('CLB không có trong dữ liệu')
    cutoff = r.get('prediction_date') or (date.fromisoformat(artifact['data_through'])+timedelta(days=1)).isoformat()
    date.fromisoformat(cutoff)
    if cutoff<=artifact['trained_through']:
        raise ValueError('Phiên bản model này chỉ dùng cho ngày sau '+artifact['trained_through'])
    p, values, details, shown, lam = pregame(artifact,r['home_team'],r['away_team'],cutoff)
    before = p.copy()
    stat_shown, stats_probability = [], None
    used, projected, mask = [], [], []
    for side in ('home','away'):
        for key in STYLE:
            value = r[side+'_stats'].get(key)
            enabled = value is not None
            mask.append(enabled)
            if enabled:
                used.append(side+'_'+key)
                # Counts are cumulative; percentages are not multiplied by time.
                projected.append(value if METRICS[key][1]=='%' else value*90/max(1,r['minute']+r['stoppage_minutes']))
            else:
                projected.append(0.)
    adjustment = 0.
    if r['mode']=='live':
        if r['minute'] or r['home_goals'] or r['away_goals']:
            p = live_probabilities(artifact,p,r['home_goals'],r['away_goals'],r['minute'],r['stoppage_minutes'])
        if used and not r['finished']:
            stats_probability, stat_shown = neighbors(projected,artifact['stat_references'],artifact['stat_scaler'],cutoff,artifact['stat_k'],artifact['half_life_days'],mask)
            elapsed = r['minute']+r['stoppage_minutes']
            adjustment = artifact['stat_weight'] * elapsed/(elapsed+30) * max(.05,1-elapsed/100)
            # A relative-likelihood correction leaves the scoreboard dominant late.
            prior = np.asarray(artifact['stat_prior'])
            p = softmax(np.log(np.clip(p,1e-12,1))+adjustment*np.log(np.clip(stats_probability,1e-12,1)/prior))
        if r['finished']:
            p = np.eye(3)[0 if r['home_goals']>r['away_goals'] else 1 if r['home_goals']==r['away_goals'] else 2]
    model = artifact['poisson']
    scaled = standard(values,model)
    contributions = []
    for i,key in enumerate(FEATURES):
        h,a = (float(model['coefficients'][side][i+1]*scaled[i]) for side in (0,1))
        contributions.append({'feature':key,'value':float(values[i]),'home_log_rate':h,'away_log_rate':a,'home_factor':math.exp(h),'away_factor':math.exp(a)})
    return {'probabilities':dict(zip(LABELS,map(float,p))), 'pregame_probabilities':dict(zip(LABELS,map(float,before))),
            'model_version':artifact['model_version'],'estimated_outcome':LABELS[int(p.argmax())], 'imputed_fields':[],
            'mode':r['mode'],'prediction_date':cutoff,'data_through':artifact['data_through'],'trained_through':artifact['trained_through'],
            'expected_goals':{'home':float(lam[0]),'away':float(lam[1])}, 'score':{'home':r['home_goals'],'away':r['away_goals'],'minute':r['minute'],'stoppage_minutes':r['stoppage_minutes'],'finished':r['finished']},
            'context':details,'features':dict(zip(FEATURES,map(float,values))), 'factors':sorted(contributions,key=lambda x:abs(x['home_log_rate']-x['away_log_rate']),reverse=True),
            'neighbors':stat_shown if stat_shown else shown,'pregame_neighbors':shown,
            'statistics_source':'manual_full_match_similarity' if used else 'historical_pregame',
            'statistics_adjustment':{'fields':used,'projected_per90':dict(zip([s+'_'+k for s in ('home','away') for k in STYLE],map(float,projected))),'weight':adjustment,'neighbor_probabilities':dict(zip(LABELS,map(float,stats_probability))) if stats_probability is not None else None,
             'validation':'Thống kê nhập được so với thống kê tổng kết trận; chưa được kiểm chứng bằng thống kê trực tiếp theo phút.' if used else None},
            'is_simulated':r['mode']=='live', 'dataset_fingerprint':artifact['dataset_fingerprint']}
