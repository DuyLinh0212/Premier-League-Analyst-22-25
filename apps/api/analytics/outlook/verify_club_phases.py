from .football_data import FootballData
from .club_phases import analyze, SEASONS

d=FootballData()
report=analyze(d)
for season in SEASONS:
    rows=report['seasons'][season]
    assert len(rows)==20
    for r in rows:
        stages=r['stages']
        assert [s['played'] for s in stages]==[10,9,10,9]
        fixtures=[m for s in stages for m in s['fixtures']]
        assert len({m['match_id'] for m in fixtures})==38
        assert [m['index'] for m in fixtures]==list(range(1,39))
        assert [m['date'] for m in fixtures]==sorted(m['date'] for m in fixtures)
        full=d.club_season(season,r['club_id'])['summary']
        assert sum(s['points'] for s in stages)==full['points']
        assert sum(s['goals_for'] for s in stages)==full['goals_for']
        assert sum(s['goals_against'] for s in stages)==full['goals_against']
        for i,s in enumerate(stages):
            for key in ['venues','opponents']:
                assert sum(g['played'] for g in s[key].values())==s['played']
                assert sum(g['points'] for g in s[key].values())==s['points']
                for g in s[key].values():
                    assert g['ppg'] is None if g['played']==0 else abs(g['ppg']-g['points']/g['played'])<1e-9
            for m in s['fixtures']:
                match=d.matches[m['match_id']]
                assert m['opponent_id'] in [match['home_club'],match['away_club']]
                assert m['opponent_id']!=r['club_id']
                rank=m['opponent_final_rank']
                assert m['opponent_group']==('top' if rank<=6 else 'middle' if rank<=14 else 'bottom')
            assert s['points']==3*s['won']+s['drawn']
            assert abs(s['ppg']-s['points']/s['played'])<1e-9
            assert s['delta_ppg'] is None if i==0 else abs(s['delta_ppg']-(s['ppg']-stages[i-1]['ppg']))<1e-9
        assert len(r['rolling'])==34
        for v in r['rolling']:
            assert v['end']-v['start']==4
            block=fixtures[v['start']-1:v['end']]
            assert len(block)==5 and v['points']==sum(m['points'] for m in block)
            assert v['ppg']==v['points']/5
for avg in report['averages']:
    originals=[r for season in report['seasons'].values() for r in season if r['club_id']==avg['club_id']]
    assert len(originals)==avg['season_count']
    for i,s in enumerate(avg['stages']):
        assert abs(s['ppg']-sum(r['stages'][i]['ppg'] for r in originals)/len(originals))<1e-9
        if i: assert s['improving_seasons']+s['declining_seasons']+s['unchanged_seasons']==len(originals)
print('PASS: 60 club-seasons, 240 phases, 38 unique fixtures each, totals and cross-season means')
