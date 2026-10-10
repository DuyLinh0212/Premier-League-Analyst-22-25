"""Executable aggregates from Danh's dashboard, against the shared Silver reader."""
from collections import Counter

SEASONS = ['2022-2023','2023-2024','2024-2025']

def analyze(reader, transfers):
    clubs = {r['club_id']:r['club_name'] for r in reader('dimensions/dim_club')}
    matches = reader('facts/fact_match_enriched')
    goals = reader('facts/fact_goal_event')
    deals = reader('facts/fact_transfer')
    teams = [{**r,'club_name':clubs[r['club_id']], 'goal_diff':r['goals_for']-r['goals_against']} for r in reader('facts/fact_team_season')]
    teams.sort(key=lambda r:(r['season'],-r['points'],-r['goal_diff'],r['club_id']))
    net = transfers['sql']['net_spend_sql']
    metrics = {}
    for season in [*SEASONS,'all']:
        d=[r for r in deals if season=='all' or r['season']==season]
        m=[r for r in matches if season=='all' or r['season']==season]
        g=[r for r in goals if season=='all' or r['season']==season]
        metrics[season]={'transfers':len(d),'spend_mil':round(sum(float(r['fee_eur'] or 0) for r in d)/1e6,2),'matches':len(m),'goals':len(g),'goals_per_match':round(len(g)/len(m),2) if m else None}
    timing=Counter((r['season'],next((label for ceiling,label in [(15,"01-15'"),(30,"16-30'"),(45,"31-45'"),(60,"46-60'"),(75,"61-75'")] if r['minute'] is not None and r['minute']<=ceiling),"76-90+'")) for r in goals)
    types=Counter((r['season'],'Trả Phí' if r['fee_eur'] is not None and float(r['fee_eur'])>0 else 'Tự Do / Mượn') for r in deals)
    profiles={}
    for cid in sorted({r['club_id'] for r in teams}):
        records=[r for r in teams if r['club_id']==cid]
        spending=[r for r in net if r['club_id']==cid]
        by_season={}
        for season in SEASONS:
            row=next((r for r in records if r['season']==season),None)
            money=next((r for r in spending if r['season']==season),{})
            # A missing Premier League season stays missing, rather than a zero season.
            by_season[season] = {'season':season,'has_played':False} if row is None else {**{k:row[k] for k in ['season','rank','points','goals_for','goals_against','goal_diff','wins','draws','losses','matches_played']},'has_played':True,**money,'wage_mil_gbp':round(float(row['annual_wage_gbp'] or 0)/1e6,2),'cost_per_point':round(money.get('gross_spend_mil_eur',0)/row['points'],2) if row['points']>0 else None}
        points=sum(r['points'] for r in records)
        spend=sum(r['gross_spend_mil_eur'] for r in spending)
        income=sum(r['sales_income_mil_eur'] for r in spending)
        profiles[cid]={'club_id':cid,'club_name':clubs[cid],'seasons_played':len(records),'total_points':points,'avg_points':round(points/len(records),1),'total_gf':sum(r['goals_for'] for r in records),'total_ga':sum(r['goals_against'] for r in records),'total_gd':sum(r['goal_diff'] for r in records),'total_spend':round(spend,2),'total_income':round(income,2),'total_net_spend':round(spend-income,2),'avg_wage_mil_gbp':round(sum(float(r['annual_wage_gbp'] or 0)/1e6 for r in records)/len(records),2),'cost_per_point':round(spend/points,2) if points else None,'seasons':by_season}
    return {'seasons_list':SEASONS,'season_metrics':metrics,'dim_clubs':[{'club_id':k,'club_name':clubs[k]} for k in profiles],'club_profiles':profiles,'top_transfers':sorted([r for r in deals if r['fee_eur'] is not None and float(r['fee_eur'])>0],key=lambda r:-float(r['fee_eur']))[:30],'transfer_type_by_season':[{'season':s,'transfer_type':t,'count':n} for (s,t),n in sorted(types.items())],'goal_timing':[{'season':s,'minute_bracket':b,'count':n} for (s,b),n in sorted(timing.items())],'team_season_data':teams,'net_spend_data':net,'roi_data':transfers['sql']['roi_sql'],'rfm_data':transfers['gmm']['clubs'],'match_results':[{k:r[k] for k in ['match_id','season','match_date','home_club_id','away_club_id','home_goals','away_goals']} for r in matches],'match_samples':sorted([{k:r[k] for k in ['match_id','season','match_date','home_club_id','away_club_id','home_goals','away_goals']} for r in matches],key=lambda r:r['match_date'],reverse=True)[:60],'method':'Danh dashboard aggregates v1; missing seasons remain missing'}
