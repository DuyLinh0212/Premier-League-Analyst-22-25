"""Chronological training, calibration, evaluation and immutable v2 artifacts."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
from datetime import date, timedelta
import numpy as np
from .forecast_v2 import FEATURES, STYLE, DEFAULT_STYLE, context, standard, rates, score_probabilities, neighbors, softmax, live_vector, score_at
from .schema import LABELS

CHECKPOINTS = ((15,0),(30,0),(45,0),(45,3),(60,0),(75,0),(85,0),(90,0),(90,3))


def evaluation(y, p):
    y,p = np.asarray(y,dtype=int),np.asarray(p,dtype=float)
    pred=p.argmax(axis=1); cm=np.zeros((3,3),dtype=int)
    for a,b in zip(y,pred): cm[a,b]+=1
    f1=[2*cm[i,i]/max(1,cm[i].sum()+cm[:,i].sum()) for i in range(3)]
    bins=[]
    for label in range(3):
        for lower in np.arange(0,1,.1):
            selected=(p[:,label]>=lower)&(p[:,label]<(lower+.1) if lower<.9 else p[:,label]<=1)
            if selected.any(): bins.append({'class':LABELS[label],'lower':round(float(lower),1),'count':int(selected.sum()),'predicted':float(p[selected,label].mean()),'observed':float((y[selected]==label).mean())})
    return {'samples':len(y),'accuracy':float((y==pred).mean()),'macro_f1':float(np.mean(f1)),
            'log_loss':float(-np.log(np.clip(p[np.arange(len(y)),y],1e-12,1)).mean()),
            'brier_score':float(np.mean(np.sum((p-np.eye(3)[y])**2,axis=1))),
            'confusion_matrix':cm.tolist(),'class_f1':dict(zip(LABELS,map(float,f1))),'reliability':bins}


def scaling(xs):
    xs=np.asarray(xs); scales=xs.std(axis=0)
    return {'means':xs.mean(axis=0).tolist(),'scales':np.where(scales>1e-8,scales,1.).tolist()}


def cosine_scaler(xs):
    xs=np.asarray(xs); low,high=xs.min(axis=0),xs.max(axis=0)
    return {'min':low.tolist(),'span':np.where(high-low>1e-8,high-low,1.).tolist()}


def fit_poisson(xs, ys, regularization):
    model=scaling(xs); x=np.c_[np.ones(len(xs)),standard(xs,model)]
    penalty=np.diag([0]+[regularization*len(xs)]*(x.shape[1]-1))
    coefficients=[]
    for target in np.asarray(ys).T:
        beta=np.zeros(x.shape[1]); beta[0]=np.log(max(.01,target.mean()))
        def loss(b):
            z=np.clip(x@b,-10,10)
            return float(np.sum(np.exp(z)-target*z)+.5*b@penalty@b)
        for _ in range(60):
            mu=np.exp(np.clip(x@beta,-10,10))
            delta=np.linalg.solve((x.T*mu)@x+penalty+np.eye(x.shape[1])*1e-8,x.T@(mu-target)+penalty@beta)
            step=1.; old=loss(beta)
            while step>1e-5 and loss(beta-step*delta)>old: step*=.5
            beta-=step*delta
            if np.max(np.abs(step*delta))<1e-7: break
        coefficients.append(beta.tolist())
    return {**model,'coefficients':coefficients,'regularization':regularization}


def fit_live(xs, labels, regularization):
    model=scaling(xs); x=np.c_[np.ones(len(xs)),standard(xs,model)]
    target=np.eye(3)[labels]; b=np.zeros((x.shape[1],3)); m=np.zeros_like(b); v=np.zeros_like(b)
    for step in range(1,1201):
        gradient=x.T@(softmax(x@b)-target)/len(x)
        gradient[1:]+=regularization*b[1:]
        m=.9*m+.1*gradient; v=.999*v+.001*gradient**2
        b-=.03*(m/(1-.9**step))/(np.sqrt(v/(1-.999**step))+1e-8)
    return {**model,'coefficients':b.T.tolist(),'regularization':regularization,'temperature':1.}


def calibrate(p,y):
    grid=np.unique(np.r_[np.geomspace(.4,3,100),1.])
    logits=np.log(np.clip(p,1e-12,1))
    losses=[evaluation(y,softmax(logits/t))['log_loss'] for t in grid]
    t=float(grid[int(np.argmin(losses))])
    return t


def goal_events(csv_path):
    with Path(csv_path).open(encoding='utf-8-sig',newline='') as stream:
        return [{**r,'minute':int(r['minute']),'stoppage_minutes':0 if r['stoppage_minutes'] in ('','\\N') else int(r['stoppage_minutes'])} for r in csv.DictReader(stream)]


def train(rows, goals, output):
    fingerprint=hashlib.sha256(json.dumps({'matches':rows,'goal_events':goals},sort_keys=True,ensure_ascii=False).encode()).hexdigest()
    # Raw source parameters are already preserved in the dataset/matches report.
    # Keep inference inputs and lineage keys here instead of duplicating that lake.
    keep=('match_id','season','match_date','home_team','away_team','home_name','away_name','label','home_goals','away_goals','home_stats','away_stats')
    rows=[{k:r[k] for k in keep} for r in rows]
    goals=[{k:g.get(k) for k in ('match_id','club_id','minute','stoppage_minutes','minute_label','is_own_goal','event_id')} for g in goals]
    rows=sorted(rows,key=lambda r:(r['match_date'],r['match_id']))
    if len({r['match_id'] for r in rows})!=len(rows): raise ValueError('Duplicate matches')
    event_ids=set(); events={r['match_id']:[] for r in rows}
    for g in goals:
        if g['event_id'] in event_ids: raise ValueError('Duplicate goal event')
        event_ids.add(g['event_id'])
        if g['match_id'] not in events: raise ValueError('Goal has no fixture')
        fixture=next(r for r in rows if r['match_id']==g['match_id'])
        if g['club_id'] not in (fixture['home_team'],fixture['away_team']): raise ValueError('Goal credited to unknown team')
        events[g['match_id']].append(g)
    for r in rows:
        observed=tuple(sum(g['club_id']==r[s+'_team'] for g in events[r['match_id']]) for s in ('home','away'))
        if observed!=(r['home_goals'],r['away_goals']): raise ValueError('Goal/score mismatch: '+r['match_id'])
    splits={name:[i for i,r in enumerate(rows) if r['season']==season] for name,season in [('train','2022-2023'),('validation','2023-2024'),('test','2024-2025')]}
    splits['tune'],splits['calibration']=splits.pop('validation')[:190],[i for i,r in enumerate(rows) if r['season']=='2023-2024'][190:]
    if any(not v for v in splits.values()): raise ValueError('Missing chronological split')
    y=np.asarray([r['label'] for r in rows]); goals_y=np.asarray([[r['home_goals'],r['away_goals']] for r in rows])
    refs_base=[{k:r[k] for k in ('match_id','match_date','season','home_team','away_team','home_name','away_name','home_goals','away_goals','label')} for r in rows]
    ti=np.asarray(splits['train']); vi=np.asarray(splits['tune']); ci=np.asarray(splits['calibration']); ei=np.asarray(splits['test'])
    candidates=[]; best=None
    for half in (90,180,365):
        xs=np.asarray([context(rows,r['home_team'],r['away_team'],r['match_date'],half)[0] for r in rows])
        for reg in (.02,.1,.5):
            model=fit_poisson(xs[ti],goals_y[ti],reg)
            probabilities=np.asarray([score_probabilities(rates(v,model)) for v in xs])
            loss=evaluation(y[vi],probabilities[vi])['log_loss']
            candidates.append({'half_life_days':half,'regularization':reg,'validation_log_loss':loss})
            if best is None or loss<best[0]: best=(loss,half,model,xs,probabilities)
    _,half,model,xs,base=best
    print('v2 Poisson candidates fitted',flush=True)
    refs=[{**r,'vector':x.tolist()} for r,x in zip(refs_base,xs)]
    scaler=cosine_scaler(xs[ti]); all_near={}
    for k in (5,15,31):
        all_near[k]=np.asarray([neighbors(xs[i],refs,scaler,rows[i]['match_date'],k,half)[0] for i in range(len(rows))])
    combinations=[]
    for k,near in all_near.items():
        for alpha in (0.,.1,.25,.4):
            p=softmax((1-alpha)*np.log(np.clip(base,1e-12,1))+alpha*np.log(np.clip(near,1e-12,1)))
            combinations.append((evaluation(y[vi],p[vi])['log_loss'],k,alpha,p))
    _,k,alpha,p=min(combinations,key=lambda x:x[:3]); temperature=calibrate(p[ci],y[ci]); pre=softmax(np.log(np.clip(p,1e-12,1))/temperature)
    pre_only_temperature=calibrate(base[ci],y[ci]); pre_only=softmax(np.log(np.clip(base,1e-12,1))/pre_only_temperature)
    # Each snapshot inherits the split of its MATCH, including all stoppage-time states.
    state_x={}; state_y={}; state_clock={}
    for split,indices in splits.items():
        vectors=[]; labels=[]; clocks=[]
        for i in indices:
            r=rows[i]
            for minute,extra in CHECKPOINTS:
                h,a=score_at(events[r['match_id']],r['home_team'],minute,extra)
                vectors.append(live_vector(pre[i],h,a,minute,extra)); labels.append(y[i]); clocks.append((minute,extra,h-a))
        state_x[split]=np.asarray(vectors); state_y[split]=np.asarray(labels); state_clock[split]=clocks
    live_candidates=[]
    for reg in (.0001,.001,.01):
        live=fit_live(state_x['train'],state_y['train'],reg)
        def infer(split): return softmax(np.c_[np.ones(len(state_x[split])),standard(state_x[split],live)]@np.asarray(live['coefficients']).T)
        val=infer('tune'); live_candidates.append((evaluation(state_y['tune'],val)['log_loss'],live))
    _,live=min(live_candidates,key=lambda x:x[0])
    def infer(split): return softmax(np.c_[np.ones(len(state_x[split])),standard(state_x[split],live)]@np.asarray(live['coefficients']).T)
    live['temperature']=calibrate(infer('calibration'),state_y['calibration'])
    live_test=softmax(np.log(np.clip(infer('test'),1e-12,1))/live['temperature'])
    # Full-match association is a separate diagnostic, never advertised as live accuracy.
    stat_x=np.asarray([[r[s+'_stats'].get(key) if r[s+'_stats'].get(key) is not None else prior for s in ('home','away') for key,prior in zip(STYLE,DEFAULT_STYLE)] for r in rows])
    stat_refs=[{**r,'vector':x.tolist()} for r,x in zip(refs_base,stat_x)]
    stat_scaler=cosine_scaler(stat_x[ti]); stat_options=[]; prior=(np.bincount(y[ti],minlength=3)+1)/(len(ti)+3)
    for sk in (5,15,31):
        near=np.asarray([neighbors(stat_x[i],stat_refs,stat_scaler,rows[i]['match_date'],sk,half)[0] for i in range(len(rows))])
        for weight in (0.,.1,.25,.5,.75):
            adjusted=softmax(np.log(np.clip(pre,1e-12,1))+weight*np.log(np.clip(near,1e-12,1)/prior))
            stat_options.append((evaluation(y[vi],adjusted[vi])['log_loss'],sk,weight,adjusted))
    _,sk,sw,stat_test=min(stat_options,key=lambda x:x[:3])
    print('v2 live score states and similarity fitted',flush=True)
    train_through=max(rows[i]['match_date'] for i in ci)
    recipe={'implementation':'forecast-v2.2','half_life':half,'poisson':model,'k':k,'alpha':alpha,'temperature':temperature,'live':live,'stat_k':sk,'stat_weight':sw}
    version='wdl2_'+hashlib.sha256((fingerprint+json.dumps(recipe,sort_keys=True)).encode()).hexdigest()[:20]
    artifact={'schema_version':2,'model_version':version,'dataset_fingerprint':fingerprint,'trained_through':train_through,
              'data_through':max(r['match_date'] for r in rows),'history':rows,'goal_events':goals,'feature_names':FEATURES,
              'half_life_days':half,'poisson':model,'references':refs,'feature_scaler':scaler,'k':k,'neighbor_weight':alpha,'temperature':temperature,
              'live':live,'stat_references':stat_refs,'stat_scaler':stat_scaler,'stat_k':sk,'stat_weight':sw,'stat_prior':prior.tolist()}
    empirical=(np.bincount(y[ti],minlength=3)+1)/(len(ti)+3)
    # A real venue-aware baseline: smoothed historical outcome frequencies, before cutoff.
    baseline=np.asarray([(np.bincount([r['label'] for r in rows if r['match_date']<rows[i]['match_date']],minlength=3)+3*empirical) /
                          (sum(r['match_date']<rows[i]['match_date'] for r in rows)+3) for i in ei])
    per_clock={}
    for minute,extra in CHECKPOINTS:
        indices=[i for i,c in enumerate(state_clock['test']) if c[:2]==(minute,extra)]
        per_clock[str(minute)+(('+'+str(extra)) if extra else '')]=evaluation(state_y['test'][indices],live_test[indices])
    score_buckets={}
    for key,check in [('level',lambda d:d==0),('home_leading',lambda d:d>0),('away_leading',lambda d:d<0)]:
        ids=[i for i,c in enumerate(state_clock['test']) if check(c[2])]
        if ids: score_buckets[key]=evaluation(state_y['test'][ids],live_test[ids])
    report={'model_version':version,'dataset_fingerprint':fingerprint,'split_counts':{key:len(v) for key,v in splits.items()},
            'split_policy':'2022-2023 train; first 190 of 2023-2024 tune; last 190 calibration; 2024-2025 test. Every snapshot stays with its match.',
            'trained_through':train_through,'data_through':artifact['data_through'],'goal_events':len(goals),'goal_score_reconciliation':'all matches exact',
            'selected':{'half_life_days':half,'k':k,'neighbor_weight':alpha,'stat_k':sk,'stat_weight':sw},'candidates':candidates,
            'pregame':evaluation(y[ei],pre[ei]),'history_only':evaluation(y[ei],pre_only[ei]),'venue_baseline':evaluation(y[ei],baseline),
            'live':evaluation(state_y['test'],live_test),'live_by_minute':per_clock,'live_by_score':score_buckets,
            'statistics_association':{**evaluation(y[ei],stat_test[ei]),'scope':'Full-match aggregate association only; NOT validated against live possession/shots.'},
            'pregame_improved_over_history_only':evaluation(y[ei],pre[ei])['log_loss']<evaluation(y[ei],pre_only[ei])['log_loss'],
            'features_use_only_prior_dates':True,'calibration_uses_separate_matches':True,'live_snapshot_matches':len(ei)}
    output=Path(output); output.mkdir(parents=True,exist_ok=True)
    for filename,value in [('forecast_v2.json',artifact),('forecast_v2_metrics.json',report)]:
        temporary=output/(filename+'.tmp'); temporary.write_text(json.dumps(value,ensure_ascii=False,allow_nan=False),encoding='utf-8'); temporary.replace(output/filename)
        target=output/'versions'/version; target.mkdir(parents=True,exist_ok=True)
        (target/filename).write_text(json.dumps(value,ensure_ascii=False,allow_nan=False),encoding='utf-8')
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser(); parser.add_argument('--output',default=str(Path(__file__).resolve().parents[2]/'data/models/match_outcome')); parser.add_argument('--goals',default=str(Path(__file__).resolve().parents[2]/'data/lake/silver/csv/facts/fact_goal_event.csv'))
    args=parser.parse_args(); rows=[json.loads(line) for line in (Path(args.output)/'matches.jsonl').read_text(encoding='utf-8').splitlines()]
    result=train(rows,goal_events(args.goals),args.output)
    print(json.dumps({k:result[k] for k in ('model_version','selected','pregame','venue_baseline','live')},ensure_ascii=True))
