"""Retrospective, explainable season ratings; never a probability forecast."""
from .football_data import num, WEIGHTS

SEASONS = ['2022-2023', '2023-2024', '2024-2025']

def percentile(x, values):
    return 10 * (sum(v < x for v in values) + .5 * sum(v == x for v in values)) / len(values)

def trend(current, previous):
    # Symmetric relative change maps -100%..+100% to 0..10; unchanged = 5.
    if current is None or previous is None: return None
    denominator = max(abs(current), abs(previous))
    return 5 if denominator == 0 else max(0, min(10, 5 + 5 * (current-previous)/denominator))

def evaluate(data, season, window=3):
    if season not in SEASONS: raise ValueError('Mùa không hợp lệ')
    if window not in (1,2,3): raise ValueError('Khoảng đánh giá phải là 1, 2 hoặc 3 mùa')
    end_index=SEASONS.index(season)+1
    selected_seasons=SEASONS[max(0,end_index-window):end_index]
    birth = {r['player_id']:num(r['birth_year']) for r in data.read_table('dimensions/dim_player')}
    records = {}
    for s in selected_seasons:
        eligible = [dict(p) for p in data.players if p['season']==s and p['minutes']>0]
        for position, weights in WEIGHTS.items():
            group = [p for p in eligible if p['position']==position]
            for p in group:
                technical = 0
                complete = all(p['metrics'][k] is not None for k in weights)
                for k,w in weights.items():
                    if k=='minutes': continue
                    if p['metrics'][k] is None: continue
                    score=percentile(p['metrics'][k],[q['metrics'][k] for q in group if q['metrics'][k] is not None])
                    if k=='goals_against90': score=10-score
                    technical += score*w/.8
                price,_,price_date=data.price(p,'valuation')
                reliability=p['minutes']/(p['minutes']+450)
                raw_technical=technical if complete else None
                technical=5+(raw_technical-5)*reliability if complete else None
                p.update(technical=technical,raw_technical=raw_technical,reliability=reliability,
                         opportunity_score=5+max(0,(raw_technical or 5)-5)*reliability*(1-min(p['minutes']/900,1)) if complete else None,
                         minutes_score=percentile(p['minutes'],[q['minutes'] for q in group]),
                         price_eur=price,price_date=price_date)
                records[(s,p['player_id'])]=p
            priced=[p for p in group if p['price_eur'] is not None and p['technical'] is not None]
            ratios=[p['technical']/(p['price_eur']/1e6) for p in priced]
            for p,ratio in zip(priced,ratios): p['value_score']=percentile(ratio,ratios)
    result=[]
    for (s,pid),p in records.items():
        if s!=season: continue
        history=[records[(old,pid)] for old in selected_seasons if (old,pid) in records]
        transitions=[]
        for before,after in zip(history,history[1:]):
            if SEASONS.index(after['season'])-SEASONS.index(before['season'])!=1: continue
            same_position=before['position']==after['position']
            transitions.append(dict(from_season=before['season'],to_season=after['season'],
                technical=trend(after['technical'],before['technical']) if same_position else None,
                minutes=trend(after['minutes'],before['minutes']),price=trend(after['price_eur'],before['price_eur'])))
        def average(key):
            values=[t[key] for t in transitions if t[key] is not None]
            return sum(values)/len(values) if values else None
        growth={k:average(k) for k in ['technical','minutes','price']}
        age=int(season[:4])+1-int(birth[pid]) if birth.get(pid) is not None else None
        def combine(parts):
            available=[(name,value,weight) for name,value,weight in parts if value is not None]
            total=sum(w for _,_,w in available)
            if not total: return None,[]
            return round(sum(v*w for _,v,w in available)/total,2),[
                dict(name=n,value=round(v,3),weight=round(w/total,6)) for n,v,w in available]
        potential,pc=combine([('Hiệu suất đã điều chỉnh mẫu nhỏ',p['technical'],.65),
            ('Tận dụng cơ hội ít phút',p['opportunity_score'],.10),
            ('Hiệu suất / định giá cùng vị trí',p.get('value_score'),.10),('Xu hướng chuyên môn',growth['technical'],.10),
            ('Xu hướng định giá',growth['price'],.05)]) if p['technical'] is not None else (None,[])
        continuity,cc=combine([('Chuyên môn hiện tại',p['technical'],.25),('Mức ra sân',p['minutes_score'],.15),
            ('Xu hướng chuyên môn',growth['technical'],.25),('Xu hướng phút',growth['minutes'],.20),
            ('Xu hướng định giá',growth['price'],.15)]) if transitions else (None,[])
        if window==1:
            continuity,cc=combine([('Chuyên môn hiện tại',p['technical'],.625),('Mức ra sân',p['minutes_score'],.375)])
        elif len(history)<window:
            continuity,cc=None,[]
        if p['technical'] is None:
            continuity,cc=None,[]
        reasons=[]
        if p['technical'] is not None and p['technical']<4: reasons.append('Đóng góp chuyên môn dưới mức 4/10 trong nhóm cùng vị trí')
        if p['opportunity_score'] is not None and p['opportunity_score']>5: reasons.append('Được cộng điểm tận dụng cơ hội: ít phút nhưng hiệu suất trên trung vị cùng vị trí')
        if growth['technical'] is not None and growth['technical']<5: reasons.append('Mức đóng góp tương đối giảm')
        if growth['minutes'] is not None and growth['minutes']<5: reasons.append('Phút thi đấu giảm')
        if growth['price'] is not None and growth['price']<5: reasons.append('Định giá giảm')
        missing=[]
        if p['minutes']<450: missing.append('Mẫu nhỏ: dưới 450 phút, chưa chắc chắn về hiệu suất')
        if p['technical'] is None: missing.append('Thiếu chỉ số chuyên môn: chưa chấm tiềm năng')
        if len(history)<window: missing.append(f'Chỉ có {len(history)}/{window} mùa đủ điều kiện; chưa chấm duy trì đa mùa')
        if age is None: missing.append('Thiếu năm sinh')
        if p['price_eur'] is None: missing.append('Thiếu định giá hợp lệ')
        if not transitions and window>1: missing.append('Không có hai mùa liên tiếp đủ dữ liệu')
        elif transitions and growth['technical'] is None: missing.append('Đổi vị trí: không so điểm chuyên môn giữa vị trí')
        if growth['price'] is None and window>1: missing.append('Thiếu cặp định giá liên tiếp')
        result.append(dict(player_id=pid,player_name=p['player_name'],clubs=p['clubs'],position=p['position'],
            age=age,young=age is not None and age<=23,potential=potential if age is not None and age<=23 else None,
            continuity=continuity,minutes=p['minutes'],price_eur=p['price_eur'],
            potential_components=pc,continuity_components=cc,reasons=reasons,missing=missing,
            history=[dict(season=h['season'],clubs=h['clubs'],position=h['position'],minutes=h['minutes'],technical=round(h['technical'],2) if h['technical'] is not None else None,
                          raw_technical=round(h['raw_technical'],2) if h['raw_technical'] is not None else None,reliability=round(h['reliability'],3),
                          price_eur=h['price_eur'],price_date=h['price_date']) for h in history]))
    return dict(season=season,window=window,selected_seasons=selected_seasons,players=result,minimum_minutes=0,
        notes=['Điểm 0–10: càng cao càng tốt. Đây là chỉ báo hồi cứu tự thiết kế, không phải xác suất hay dự báo đào thải.',
        'Khoảng đánh giá tính lùi từ mùa chọn. Chỉ dùng các mùa trong khoảng, nối theo ID cầu thủ xuyên CLB. Không lấy mùa ngoài khoảng để bù lịch sử thiếu.',
        'Chọn 1 mùa: điểm duy trì là ảnh chụp hiện tại (62,5% chuyên môn + 37,5% mức ra sân), không kết luận xu hướng sa sút. Chọn 2/3 mùa: phải có đủ số mùa đủ điều kiện mới chấm duy trì.',
        'Trẻ: tuổi xấp xỉ = năm kết thúc mùa − năm sinh, ≤23. Không có ngày sinh nên tuổi có thể lệch một tuổi.',
        'Giữ mọi mùa có phút >0, kể cả dưới 450 phút. Thiếu chỉ số được để trống, không điền 0. Dưới 450 phút ghi mẫu nhỏ; ít phút không đồng nghĩa ít tiềm năng.',
        'Điều chỉnh mẫu nhỏ: r = phút/(phút + 450); chuyên môn điều chỉnh = 5 + r × (điểm hiệu suất gốc − 5). Đây là quy tắc làm ổn định tự thiết kế, không phải xác suất thống kê.',
        'Chuyên môn: percentile chỉ số /90 trong cùng mùa/vị trí; trọng số như P/P, bỏ phần phút và chuẩn hóa về 10.',
        'Tiềm năng: hiệu suất điều chỉnh 65%, tận dụng cơ hội 10%, hiệu suất/định giá 10%, xu hướng chuyên môn 10%, xu hướng định giá 5%. Không cộng điểm trực tiếp chỉ vì đá nhiều phút.',
        'Tận dụng cơ hội = 5 + max(0, hiệu suất gốc − 5) × r × (1 − min(phút/900,1)). Chỉ hiệu suất tốt hơn trung vị mới được thưởng, không thưởng riêng việc ít phút. Giá trị trung tính =5.',
        'Duy trì: chuyên môn 25%, mức ra sân 15%, xu hướng chuyên môn 25%, xu hướng phút 20%, xu hướng định giá 15%. Chưa có hai mùa liên tiếp: không chấm.',
        'Xu hướng mỗi cặp mùa liên tiếp = 5 + 5 × (mới − cũ)/max(|mới|,|cũ|); không đổi =5. Có hai cặp thì lấy trung bình. Đổi vị trí: bỏ so chuyên môn.',
        'Thiếu thành phần: bỏ thành phần đó và chuẩn hóa lại trọng số; chi tiết hiển thị trọng số thực dùng. Không điền 0.',
        'Giá: định giá gần nhất trong 365 ngày trước 30/06 cuối mùa, không dùng dữ liệu mùa sau. Giá giảm cũng có thể do tuổi/thị trường, không chứng minh bị đào thải.'])
