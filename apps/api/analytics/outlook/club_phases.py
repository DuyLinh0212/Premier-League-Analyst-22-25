"""Four chronological fixture phases, not official matchweeks."""
from collections import defaultdict

SEASONS=['2022-2023','2023-2024','2024-2025']
PHASES=[('Khởi đầu',1,10),('Giữa lượt đi',11,19),('Đầu lượt về',20,29),('Nước rút',30,38)]

def group_stats(fixtures):
    n=len(fixtures)
    points=sum(m['points'] for m in fixtures)
    return dict(played=n,points=points,ppg=points/n if n else None,
        gf_per_match=sum(m['goals_for'] for m in fixtures)/n if n else None,
        ga_per_match=sum(m['goals_against'] for m in fixtures)/n if n else None)

def analyze(data):
    seasons={}
    clubs={}
    for season in SEASONS:
        club_ids=sorted({m[side] for m in data.matches.values() if m['season']==season for side in ['home_club','away_club']})
        standings=data.club_season(season)['standings']
        ranks={r['club_id']:r['rank'] for r in standings}
        reports=[]
        for cid in club_ids:
            timeline=data.club_season(season,cid)['timeline']
            assert len(timeline)==38, f'Incomplete season {season} {cid}'
            for m in timeline:
                match=data.matches[m['match_id']]
                opponent=match['away_club'] if match['home_club']==cid else match['home_club']
                rank=ranks[opponent]
                m.update(opponent_id=opponent,opponent_final_rank=rank,
                    opponent_group='top' if rank<=6 else 'middle' if rank<=14 else 'bottom')
            rolling=[]
            for i in range(4,len(timeline)):
                block=timeline[i-4:i+1]
                rolling.append(dict(index=i+1,date=block[-1]['date'],opponent=block[-1]['opponent'],
                    start=i-3,end=i+1,ppg=sum(m['points'] for m in block)/5,
                    points=sum(m['points'] for m in block),results=''.join(m['result'] for m in block)))
            stages=[]
            for index,(name,start,end) in enumerate(PHASES):
                fixtures=timeline[start-1:end]
                n=len(fixtures)
                points=sum(m['points'] for m in fixtures)
                gf=sum(m['goals_for'] for m in fixtures)
                ga=sum(m['goals_against'] for m in fixtures)
                won=sum(m['result']=='W' for m in fixtures)
                drawn=sum(m['result']=='D' for m in fixtures)
                ppg=points/n
                previous=stages[-1] if stages else None
                stage=dict(phase=index+1,name=name,start=start,end=end,played=n,points=points,won=won,drawn=drawn,lost=n-won-drawn,
                    ppg=ppg,win_rate=won/n*100,goals_for=gf,goals_against=ga,gf_per_match=gf/n,ga_per_match=ga/n,
                    delta_ppg=ppg-previous['ppg'] if previous else None,
                    venues={key:group_stats([m for m in fixtures if m['venue']==label]) for key,label in [('home','Sân nhà'),('away','Sân khách')]},
                    opponents={key:group_stats([m for m in fixtures if m['opponent_group']==key]) for key in ['top','middle','bottom']},
                    fixtures=fixtures)
                stages.append(stage)
            clubs[cid]=data.clubs.get(cid,cid)
            reports.append(dict(club_id=cid,club_name=clubs[cid],season=season,stages=stages,rolling=rolling))
        seasons[season]=reports
    grouped=defaultdict(list)
    for reports in seasons.values():
        for report in reports: grouped[report['club_id']].append(report)
    averages=[]
    for cid,reports in grouped.items():
        stages=[]
        for index,(name,start,end) in enumerate(PHASES):
            rows=[r['stages'][index] for r in reports]
            stage=dict(phase=index+1,name=name,start=start,end=end,season_count=len(rows),
                **{k:sum(r[k] for r in rows)/len(rows) for k in ['ppg','win_rate','gf_per_match','ga_per_match']})
            changes=[r['delta_ppg'] for r in rows if r['delta_ppg'] is not None]
            stage.update(delta_ppg=sum(changes)/len(changes) if changes else None,
                improving_seasons=sum(x>1e-9 for x in changes),declining_seasons=sum(x< -1e-9 for x in changes),
                unchanged_seasons=sum(abs(x)<=1e-9 for x in changes))
            stages.append(stage)
        averages.append(dict(club_id=cid,club_name=clubs[cid],season_count=len(reports),available_seasons=[r['season'] for r in reports],stages=stages))
    return dict(seasons=seasons,averages=averages,clubs=[dict(club_id=cid,club_name=name) for cid,name in sorted(clubs.items(),key=lambda x:x[1])],
        phases=[dict(phase=i+1,name=n,start=s,end=e) for i,(n,s,e) in enumerate(PHASES)],
        notes=['Không có cột vòng đấu gốc: chia theo thứ tự ngày thi đấu của mỗi CLB, không phải vòng chính thức. Trận đá bù có thể nằm ở giai đoạn khác vòng gốc.',
        'Bốn giai đoạn: trận 1–10, 11–19, 20–29, 30–38. So điểm/trận và bàn/trận vì số trận mỗi giai đoạn là 10, 9, 10, 9.',
        'Điểm từ kết quả thắng 3, hòa 1, thua 0; không tính án trừ điểm. Tăng/giảm = điểm/trận giai đoạn này − giai đoạn liền trước.',
        'So 3 mùa: trung bình mỗi mùa có dữ liệu, không coi mùa ngoài Premier League là 0. Số mùa hiển thị rõ; có thể lọc chỉ CLB đủ 3 mùa.',
        'Đối thủ: nhóm đầu hạng 1–6, giữa 7–14, cuối 15–20 theo bảng cuối mùa tính từ kết quả, chưa áp dụng án trừ điểm. Đây là phân nhóm hồi cứu, không phải sức mạnh tại ngày đá. Không dùng làm dự báo hay điểm hiệu chỉnh.',
        'So 2 CLB bằng trung bình: chỉ dùng các mùa cả hai cùng có dữ liệu. Số mùa chung hiển thị rõ; không có mùa chung thì không tính.',
        'Đường 5 trận: cửa sổ trượt đủ 5 trận, từ trận thứ 5 đến 38; điểm/trận = tổng điểm 5 trận /5. Không tự điền bốn trận đầu.',
        'Tăng tốc/sa sút là mô tả kết quả, không chứng minh nguyên nhân. Lịch đối thủ và sân nhà/sân khách có thể ảnh hưởng; chỉ 3 mùa chưa đủ kết luận quy luật lâu dài.'])
