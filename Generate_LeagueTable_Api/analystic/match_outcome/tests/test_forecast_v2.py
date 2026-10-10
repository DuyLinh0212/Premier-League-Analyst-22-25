import copy
import json
import math
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
import numpy as np
from fastapi.testclient import TestClient
from match_outcome.forecast_v2 import context, forecast, score_at, validate_request, score_probabilities
from match_outcome.api import app

OUTPUT=Path(__file__).resolve().parents[1]/'outputs'

class ForecastV2Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.artifact=json.loads((OUTPUT/'forecast_v2.json').read_text(encoding='utf-8'))
        cls.metrics=json.loads((OUTPUT/'forecast_v2_metrics.json').read_text(encoding='utf-8'))
        cls.request={'home_team':'LIV','away_team':'ARS','prediction_date':'2025-05-26'}

    def test_as_of_features_and_neighbors_do_not_see_future(self):
        cutoff='2024-11-01'
        original=forecast({**self.request,'prediction_date':cutoff},self.artifact)
        changed=copy.deepcopy(self.artifact)
        for r in changed['history']:
            if r['match_date']>=cutoff:
                r['home_goals']=30; r['away_goals']=0; r['home_stats']['shots']=999
        for r in changed['references']+changed['stat_references']:
            if r['match_date']>=cutoff:
                r['label']=0; r['vector']=[999]*len(r['vector'])
        after=forecast({**self.request,'prediction_date':cutoff},changed)
        self.assertEqual(original['features'],after['features'])
        self.assertEqual(original['neighbors'],after['neighbors'])
        self.assertEqual(original['probabilities'],after['probabilities'])
        self.assertTrue(all(r['match_date']<cutoff for r in original['neighbors']+original['context']['head_to_head']))

    def test_default_and_zero_minute_are_identical(self):
        before=forecast(self.request,self.artifact)
        live=forecast({**self.request,'mode':'live'},self.artifact)
        self.assertEqual(before['probabilities'],live['probabilities'])
        self.assertEqual(before['score']['home'],0)
        self.assertEqual(before['prediction_date'],'2025-05-26')
        default=forecast({'home_team':'LIV','away_team':'ARS'},self.artifact)
        self.assertEqual(default['prediction_date'],'2025-05-26')

    def test_score_time_changes_and_end_is_exact(self):
        early=forecast({**self.request,'mode':'live','minute':10,'home_goals':1},self.artifact)
        late=forecast({**self.request,'mode':'live','minute':85,'home_goals':1},self.artifact)
        self.assertGreater(late['probabilities']['home_win'],early['probabilities']['home_win'])
        minute90=forecast({**self.request,'mode':'live','minute':90,'home_goals':1},self.artifact)
        self.assertLess(minute90['probabilities']['home_win'],1)
        for h,a,label in [(2,1,'home_win'),(0,0,'draw'),(1,2,'away_win')]:
            result=forecast({**self.request,'mode':'live','minute':90,'finished':True,'home_goals':h,'away_goals':a},self.artifact)
            self.assertEqual(result['probabilities'][label],1)

    def test_stats_match_partial_fields_and_zero_is_observed(self):
        blank=forecast({**self.request,'mode':'live','minute':60},self.artifact)
        stats=forecast({**self.request,'mode':'live','minute':60,'home_stats':{'possession':65,'shots':14,'shots_on_target':6},'away_stats':{'possession':35,'shots':4,'shots_on_target':0}},self.artifact)
        self.assertNotEqual(blank['probabilities'],stats['probabilities'])
        self.assertIn('away_shots_on_target',stats['statistics_adjustment']['fields'])
        self.assertNotIn('home_crosses',stats['statistics_adjustment']['fields'])
        self.assertIn('chưa được kiểm chứng',stats['statistics_adjustment']['validation'])
        self.assertTrue(all(0<=r['similarity']<=1+1e-12 for r in stats['neighbors']))
        self.assertTrue(all(r['match_date']<self.request['prediction_date'] for r in stats['neighbors']))

    def test_stoppage_and_own_goals_are_credited_once(self):
        goals=[{'club_id':'LIV','minute':45,'stoppage_minutes':2,'is_own_goal':True},{'club_id':'ARS','minute':46,'stoppage_minutes':0},{'club_id':'LIV','minute':90,'stoppage_minutes':3}]
        self.assertEqual(score_at(goals,'LIV',45,1),(0,0))
        self.assertEqual(score_at(goals,'LIV',45,3),(1,0))
        self.assertEqual(score_at(goals,'LIV',46),(1,1))
        self.assertEqual(score_at(goals,'LIV',90,3),(2,1))
        self.assertEqual(self.metrics['goal_events'],3445)
        self.assertEqual(len(self.artifact['history']),1140)
        self.assertEqual(self.metrics['split_counts'],{'train':380,'tune':190,'calibration':190,'test':380})
        self.assertEqual(self.metrics['live']['samples'],380*9)

    def test_invalid_and_sparse_inputs(self):
        for changes in [{'home_team':'ARS'},{'minute':-1,'mode':'live'},{'home_goals':1},{'mode':'live','stoppage_minutes':2,'minute':60},{'mode':'live','minute':10,'home_stats':{'shots':2,'shots_on_target':3}},{'mode':'live','minute':20,'home_stats':{'possession':60},'away_stats':{'possession':60}},{'prediction_date':'2023-01-01'}]:
            with self.assertRaises(ValueError): forecast({**self.request,**changes},self.artifact)
        # A promoted/relegated club with no recent matches uses shrinkage, not NaN.
        result=forecast({**self.request,'home_team':'SOU','prediction_date':'2027-08-01'},self.artifact)
        self.assertTrue(all(math.isfinite(p) for p in result['probabilities'].values()))
        for h,a in [(0.05,0.05),(2,3),(12,12)]:
            self.assertAlmostEqual(float(score_probabilities((h,a)).sum()),1,places=12)

    def test_db_and_kafka_errors_do_not_return_success(self):
        def report(kind,*args,**kwargs):
            return (self.artifact if kind=='forecast_v2' else self.metrics,{'run_id':'test_bundle'})
        with TestClient(app,raise_server_exceptions=False) as client, patch('analytics.store.report',side_effect=report), patch('match_outcome.api.save_user_analysis') as saved:
            producer=Mock(); app.state.producer=producer
            saved.side_effect=lambda kind,event,result,simulated:{**result,'run_id':'saved_v2','persistence':'saved'}
            config=client.get('/prediction/config').json()
            self.assertEqual(config['default_date'],'2025-05-26')
            good=client.post('/predict/v2',json=self.request)
            self.assertEqual(good.status_code,200)
            self.assertEqual(good.json()['persistence'],'saved')
            sent=producer.send.call_args.kwargs['value']
            self.assertEqual(sent['schema_version'],2)
            self.assertEqual(sent['prediction_date'],'2025-05-26')
            saved.side_effect=ConnectionError('DB offline')
            self.assertEqual(client.post('/predict/v2',json=self.request).status_code,503)
            producer.send.side_effect=ConnectionError('Kafka offline')
            self.assertEqual(client.post('/predict/v2',json=self.request).status_code,503)
            self.assertEqual(client.post('/predict/v2',json={**self.request,'home_goals':True}).status_code,422)

if __name__=='__main__': unittest.main()
