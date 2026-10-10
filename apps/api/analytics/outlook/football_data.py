"""Read-only analysis of the checksum-verified Silver CSV export."""
import csv
import math
import os
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(os.getenv('SILVER_CSV_DIR', str(Path(__file__).resolve().parents[4] / 'data/lake/silver/csv')))

def read_table(name):
    with (ROOT / (name + '.csv')).open(encoding='utf-8-sig', newline='') as stream:
        return list(csv.DictReader(stream))

def num(value):
    if value in (None, '', '\\N'): return None
    value = float(value)
    return value if math.isfinite(value) else None

def match_summary(row):
    teams = []
    for side in ('home','away'):
        def n(column): return num(row.get(side+'_'+column))
        teams.append(dict(club_id=row[side+'_club_id'],goals=num(row[side+'_goals']),
            shots=n('shooting_shots'),shots_on_target=n('shooting_shots_on_target'),
            yellow_cards=n('misc_yellow_cards'),red_cards=n('misc_red_cards'),
            possession=n('possession'),passes=None))
    return dict(match_id=row['match_id'],season=row['season'],match_date=row['match_date'],
        home_club=row['home_club_id'],away_club=row['away_club_id'],
        home_score=int(row['home_goals']),away_score=int(row['away_goals']),teams=teams)

# These are explanatory analytical weights, not an official player rating.
WEIGHTS = {
    'FW': {'non_penalty_goals90':.60,'assists90':.25,'shots_on_target90':.15},
    'MF': {'assists90':.35,'non_penalty_goals90':.25,'tackles90':.20,'interceptions90':.20},
    'DF': {'tackles90':.40,'interceptions90':.40,'assists90':.20},
    'GK': {'save_pct':.50,'clean_sheets90':.25,'goals_against90':.25},
}

WEIGHTS = {position: {**{metric: weight * .8 for metric, weight in weights.items()}, 'minutes': .2}
           for position, weights in WEIGHTS.items()}

class FootballData:
    def __init__(self, reader=read_table):
        self.read_table = reader
        read_table = reader
        self.matches={r['match_id']:match_summary(r) for r in read_table('facts/fact_match_enriched')}
        self.clubs={r['club_id']:r['club_name'] for r in read_table('dimensions/dim_club')}
        bridges=read_table('bridges/bridge_transfermarkt_player')
        self.bridge={r['player_id']:r['tm_player_id'] for r in bridges}
        assert len(self.bridge)==len(bridges)==len({r['tm_player_id'] for r in bridges})
        self.valuations=defaultdict(list)
        for r in read_table('facts/fact_player_valuation'):
            value=num(r['market_value_eur'])
            if value is not None:
                self.valuations[r['tm_player_id']].append((date.fromisoformat(r['valuation_date']),value))
        for values in self.valuations.values(): values.sort()
        self.transfers=read_table('facts/fact_transfer')
        grouped=defaultdict(list)
        for r in read_table('facts/fact_player_season'): grouped[(r['season'],r['player_id'])].append(r)
        self.players=[]
        for (season,pid), entries in grouped.items():
            def total(key):
                values=[num(r.get(key)) for r in entries]
                return sum(values) if all(v is not None for v in values) else None
            minutes=total('minutes')
            if minutes is None or minutes<=0: continue
            primary=max(entries,key=lambda r:num(r['minutes']) or 0)
            position=primary['position']
            if position not in WEIGHTS: continue
            metrics={'minutes':minutes}
            for source,target in [('non_penalty_goals','non_penalty_goals90'),('assists','assists90'),('shooting_shots_on_target','shots_on_target90'),('misc_tackles_won','tackles90'),('misc_interceptions','interceptions90')]:
                value=total(source)
                metrics[target]=None if value is None else value*90/minutes
            gk_minutes=total('gk_minutes')
            saves, against=total('gk_saves'),total('gk_shots_on_target_against')
            metrics['save_pct']=saves/against*100 if saves is not None and against and against>0 else None
            for source,target in [('gk_clean_sheets','clean_sheets90'),('gk_goals_against','goals_against90')]:
                value=total(source)
                metrics[target]=value*90/gk_minutes if value is not None and gk_minutes and gk_minutes>0 else None
            self.players.append(dict(player_id=pid,tm_player_id=self.bridge.get(pid),player_name=primary['player_name'],season=season,position=position,
                club_ids=sorted({r['club_id'] for r in entries}),clubs=' / '.join(self.clubs.get(cid,cid) for cid in sorted({r['club_id'] for r in entries})),
                minutes=minutes,goals=total('goals'),assists=total('assists'),metrics=metrics))

    def club_season(self, season, club_id=None):
        matches = [m for m in self.matches.values() if m['season']==season]
        clubs = sorted({m[side] for m in matches for side in ['home_club','away_club']},key=lambda cid:self.clubs.get(cid,cid))
        if not matches: raise ValueError('Mùa không hợp lệ')
        club_id = club_id or ('MUN' if 'MUN' in clubs else clubs[0])
        if club_id not in clubs: raise ValueError('CLB không thi đấu trong mùa này')
        standings=[]
        selected=[]
        for cid in clubs:
            fixtures=sorted([m for m in matches if cid in [m['home_club'],m['away_club']]],key=lambda m:(m['match_date'],m['match_id']))
            points=gf_total=ga_total=won=drawn=lost=0
            timeline=[]
            for index,m in enumerate(fixtures,1):
                home=m['home_club']==cid
                gf=m['home_score'] if home else m['away_score']
                ga=m['away_score'] if home else m['home_score']
                earned=3 if gf>ga else 1 if gf==ga else 0
                points+=earned; gf_total+=gf; ga_total+=ga
                won+=gf>ga; drawn+=gf==ga; lost+=gf<ga
                t=next(t for t in m['teams'] if t['club_id']==cid)
                timeline.append(dict(match_id=m['match_id'],index=index,date=m['match_date'],opponent=self.clubs.get(m['away_club'] if home else m['home_club']),venue='Sân nhà' if home else 'Sân khách',goals_for=gf,goals_against=ga,result='W' if gf>ga else 'D' if gf==ga else 'L',points=earned,cumulative_points=points,**{k:t[k] for k in ['shots','shots_on_target','yellow_cards','red_cards','possession']}))
            row=dict(club_id=cid,club_name=self.clubs.get(cid,cid),played=len(fixtures),won=won,drawn=drawn,lost=lost,goals_for=gf_total,goals_against=ga_total,goal_difference=gf_total-ga_total,points=points)
            standings.append(row)
            if cid==club_id: selected=timeline; summary=row
        standings.sort(key=lambda r:(-r['points'],-r['goal_difference'],-r['goals_for'],r['club_id']))
        for rank,r in enumerate(standings,1): r['rank']=rank
        summary.update(win_rate=round(summary['won']/summary['played']*100,1),points_per_match=round(summary['points']/summary['played'],2),last_five=' '.join(r['result'] for r in selected[-5:]))
        for field in ['shots','shots_on_target','yellow_cards','red_cards']:
            values=[m[field] for m in selected]
            summary[field]=sum(values) if all(v is not None for v in values) else None
        return dict(season=season,club_id=club_id,clubs=[dict(club_id=c,club_name=self.clubs.get(c,c)) for c in clubs],summary=summary,timeline=selected,standings=standings,source='fact_match_enriched',note='Điểm tính từ thắng 3, hòa 1, thua 0; chưa tính án trừ điểm. Thứ hạng theo điểm, hiệu số và bàn thắng; nếu vẫn bằng nhau, thứ tự ID chỉ phục vụ hiển thị. Trận xếp theo ngày thi đấu thực, không phải số vòng lịch gốc.')

    def price(self,p,basis):
        end=date(int(p['season'][:4])+1,6,30)
        if basis=='valuation':
            values=[v for v in self.valuations.get(p['tm_player_id'],[]) if end-timedelta(days=365)<=v[0]<=end]
            if not values: return None,'Không có định giá trong 365 ngày trước cuối mùa',None
            day,value=values[-1]
            if value<=0: return None,'Định giá bằng 0: không chia để tính P/P',str(day)
            return value,'Định giá gần nhất trước hoặc tại 30/06 cuối mùa',str(day)
        start=date(int(p['season'][:4]),7,1)
        transfers=[r for r in self.transfers if r['player_id']==p['player_id'] and r['to_club_id'] in p['club_ids'] and r['season']==p['season'] and start<=date.fromisoformat(r['transfer_date'])<=end]
        positive=[r for r in transfers if num(r['fee_eur']) is not None and num(r['fee_eur'])>0]
        if not positive: return None,'Không có phí chuyển đến CLB hợp lệ > 0 trong mùa',None
        # Sum known incoming costs for multiple transfers; unknown fees invalidate denominator.
        if any(num(r['fee_eur']) is None for r in transfers): return None,'Có phí chuyển nhượng thiếu trong mùa',None
        return sum(num(r['fee_eur']) for r in positive),'Tổng phí chuyển đến các CLB cầu thủ thi đấu trong mùa',max(r['transfer_date'] for r in positive)

    def pp(self,season,basis='valuation',min_minutes=900):
        pool=[dict(p) for p in self.players if p['season']==season and p['minutes']>=min_minutes]
        eligible=[]
        for p in pool:
            weights=WEIGHTS[p['position']]
            if any(p['metrics'][k] is None for k in weights): continue
            eligible.append(p)
        for position,weights in WEIGHTS.items():
            group=[p for p in eligible if p['position']==position]
            for p in group:
                p['components']=[]
                score=0
                for key,weight in weights.items():
                    values=[q['metrics'][key] for q in group]
                    x=p['metrics'][key]
                    # Midrank percentile: equal values yield equal scores.
                    rank=(sum(v<x for v in values)+.5*sum(v==x for v in values))/len(values)
                    if key=='goals_against90': rank=1-rank
                    component=rank*100
                    score+=weight*component
                    p['components'].append(dict(metric=key,value=round(x,4),percentile=round(component,2),weight=weight,cohort_size=len(group)))
                p['performance']=round(score,4)
        ranked=[]
        exclusions=defaultdict(int)
        for p in eligible:
            if basis in ('best','worst'):
                price,reason,price_date=self.price(p,'valuation')
                p.update(price_eur=price,price_date=price_date,price_reason=reason,pp=None)
                ranked.append(p)
                continue
            price,reason,price_date=self.price(p,basis)
            if price is None:
                exclusions[reason]+=1
                continue
            p.update(price_eur=price,price_date=price_date,price_reason=reason,pp=round(p['performance']/(price/1000000),6))
            ranked.append(p)
        if basis in ('best','worst'):
            ranked.sort(key=lambda p:((1 if basis=='worst' else -1)*p['performance'],p['player_id']))
        else:
            ranked.sort(key=lambda p:(-p['pp'],-p['performance'],p['player_id']))
        lineup={pos:[p for p in ranked if p['position']==pos][:n] for pos,n in [('GK',1),('DF',4),('MF',3),('FW',3)]}
        return dict(season=season,basis=basis,min_minutes=min_minutes,players=ranked,lineup=lineup,
            coverage=dict(season_players=sum(p['season']==season for p in self.players),minutes_eligible=len(pool),metrics_eligible=len(eligible),priced=len(ranked),excluded_prices=dict(exclusions)),
            formula='P/P = điểm đóng góp (0–100) / giá tính bằng triệu EUR. Điểm gồm 80% chỉ số theo vị trí và 20% mức độ ra sân (percentile số phút). Chỉ số trên sân quy về /90 phút, xếp percentile cùng vị trí, mùa và ngưỡng phút. Đây là công thức phân tích tự thiết kế, không phải điểm chính thức.',
            notes=['So sánh P/P trong cùng vị trí. Điểm phòng ngự/thủ môn chịu ảnh hưởng đội bóng.', 'Định giá cuối mùa là phân tích hồi cứu; không dùng để dự báo trước mùa.', 'Phí chuyển nhượng chỉ xếp hạng cầu thủ có thương vụ trong mùa; không đại diện toàn bộ cầu thủ.', 'Vị trí chỉ có GK/DF/MF/FW; sân 4–3–3 không suy diễn hậu vệ trái/phải.', 'Cầu thủ nhiều CLB được cộng chỉ số trong cùng mùa; vị trí theo CLB có nhiều phút nhất.', 'Đội hình chuyển nhượng dùng đóng góp toàn mùa tại các CLB trong dữ liệu; đây không phải phép đo riêng phần đóng góp sau ngày gia nhập CLB mới.'],weights=WEIGHTS)
