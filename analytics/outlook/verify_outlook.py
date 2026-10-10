from .football_data import FootballData
from .player_outlook import evaluate, SEASONS, trend

data=FootballData()
assert trend(100,100)==5 and trend(0,100)==0 and trend(100,0)==10
for season in SEASONS:
    result=evaluate(data,season)
    rows=result['players']
    assert len({p['player_id'] for p in rows})==len(rows)
    for p in rows:
        assert p['minutes']>0
        assert all(h['season']<=season for h in p['history'])
        for h in p['history']:
            if h['price_date']: assert h['price_date']<=str(int(h['season'][:4])+1)+'-06-30'
        assert p['potential'] is None or p['young']
        if season==SEASONS[0]: assert p['continuity'] is None
        for key in ['potential','continuity']:
            if p[key] is None: continue
            assert 0<=p[key]<=10
            cs=p[key+'_components']
            assert abs(sum(c['weight'] for c in cs)-1)<.00001
            assert abs(sum(c['weight']*c['value'] for c in cs)-p[key])<.011
    print(season,len(rows),'young',sum(p['young'] for p in rows),'continuity',sum(p['continuity'] is not None for p in rows))
# Future seasons and valuations cannot affect an earlier report.
before=evaluate(data,SEASONS[0])
data.players=[p for p in data.players if p['season']==SEASONS[0]]
assert before==evaluate(data,SEASONS[0])
print('PASS: score bounds, formulas, missing history and no future-season leakage')
data=FootballData()
for window in (1,2,3):
    report=evaluate(data,'2024-2025',window)
    assert len(report['selected_seasons'])==window
    for p in report['players']:
        assert len(p['history'])<=window
        assert all(h['season'] in report['selected_seasons'] for h in p['history'])
        if window==1 and p['potential'] is not None: assert p['continuity'] is not None
        elif len(p['history'])<window: assert p['continuity'] is None
    print('WINDOW',window,'scored',sum(p['continuity'] is not None for p in report['players']))
