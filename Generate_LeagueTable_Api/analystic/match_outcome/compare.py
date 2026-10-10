"""Compare LR, classical KNN and Spark RF using the same chronological split."""
import argparse
import copy
import csv
import hashlib
import html
import json
import os
import shutil
from pathlib import Path
import numpy as np
from pyspark.sql import SparkSession, Window, functions as F
from pyspark.ml.classification import RandomForestClassifier, LogisticRegressionModel
from pyspark.ml.linalg import Vectors
from .inference import vector, base_logits, softmax, predict
from .schema import LABELS

def write_report(output, report):
    output = Path(output)
    (output/'algorithm_comparison.json').write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
    (output/'match_outcome.html').write_text("<!doctype html><html lang='vi'><meta charset='utf-8'><title>So sánh model kết quả trận</title>"
        "<style>body{font:16px system-ui;max-width:1000px;margin:40px auto;padding:20px}pre{white-space:pre-wrap}</style>"
        "<h1>Ước tính thắng – hòa – thua</h1><p>Model được chọn theo log loss validation, hiệu chỉnh trên tập calibration và kiểm thử riêng mùa 2024–2025. Đây là thống kê tổng kết trận; khả năng nhận diện hòa còn yếu.</p><pre>"
        + html.escape(json.dumps(report,ensure_ascii=False,indent=2)) + "</pre></html>",encoding='utf-8')

def export_tree(node):
    if node.getClass().getSimpleName() == 'LeafNode':
        counts = np.asarray(list(node.impurityStats().stats()), dtype=float)
        return {'probabilities': (counts / counts.sum()).tolist()}
    split = node.split()
    return {'feature_index': int(split.featureIndex()), 'threshold': float(split.threshold()),
            'left': export_tree(node.leftChild()), 'right': export_tree(node.rightChild())}

def spark_knn_probabilities(spark, xs, artifact):
    refs = spark.createDataFrame([(i,int(y),list(map(float,x))) for i,(y,x) in enumerate(zip(artifact['reference_labels'],artifact['reference_vectors']))], 'reference_id int, label int, reference array<double>')
    queries = spark.createDataFrame([(i,list(map(float,x))) for i,x in enumerate(xs)], 'index int, query array<double>')
    pairs = queries.crossJoin(F.broadcast(refs)).withColumn('distance',F.aggregate(F.zip_with('query','reference',lambda q,r:(q-r)*(q-r)),F.lit(0.0),lambda a,b:a+b))
    closest = pairs.withColumn('order',F.row_number().over(Window.partitionBy('index').orderBy('distance','reference_id'))).where(F.col('order') <= artifact['k'])
    values = closest.groupBy('index').agg(*[F.sum(F.when(F.col('label')==label,1).otherwise(0)).alias('count'+str(label)) for label in range(3)]).collect()
    result = np.zeros((len(xs),3))
    for row in values:
        result[row['index']] = (np.asarray([row['count'+str(i)] for i in range(3)]) + artifact['smoothing']*np.asarray(artifact['class_prior']))/(artifact['k']+artifact['smoothing'])
    return result

def compare(output):
    from .train import metrics
    output = Path(output)
    baseline = json.loads((output/'model.json').read_text(encoding='utf-8'))
    fresh_baseline = baseline.get('algorithm','logistic_regression') == 'logistic_regression'
    if baseline.get('algorithm','logistic_regression') != 'logistic_regression':
        baseline = json.loads((output/'logistic_baseline/model.json').read_text(encoding='utf-8'))
    report = json.loads((output/'metrics.json').read_text(encoding='utf-8'))
    if 'comparison' in report:
        report = json.loads((output/'logistic_baseline/metrics.json').read_text(encoding='utf-8'))
    rows = [json.loads(line) for line in (output/'matches.jsonl').read_text(encoding='utf-8').splitlines()]
    historical = [r for r in rows if r['season']=='2022-2023']
    validation = sorted([r for r in rows if r['season']=='2023-2024'],key=lambda r:(r['match_date'],r['match_id']))
    splits = {'train':historical,'tune':validation[:190],'calibration':validation[190:],'test':[r for r in rows if r['season']=='2024-2025']}
    xs = {k:np.stack([vector(r,baseline)[0] for r in v]) for k,v in splits.items()}
    ys = {k:np.asarray([r['label'] for r in v]) for k,v in splits.items()}
    prior = np.bincount(ys['train'],minlength=3)/len(historical)
    candidates = []
    best = copy.deepcopy(baseline)
    best['algorithm'] = 'logistic_regression'
    best_model = None
    lr_loss = min(x['log_loss'] for x in report['candidates'])
    for c in report['candidates']:
        candidates.append({'algorithm':'logistic_regression','parameters':{'reg_param':c['reg_param']},'validation_log_loss':c['log_loss'],'validation_accuracy':c['accuracy'],'validation_macro_f1':c['macro_f1']})
    best_loss = lr_loss
    def consider(artifact, parameters, model=None):
        nonlocal best, best_loss, best_model
        p = softmax(np.stack([base_logits(x,artifact) for x in xs['tune']]))
        score = metrics(ys['tune'],p)
        candidates.append({'algorithm':artifact['algorithm'],'parameters':parameters,'validation_log_loss':score['log_loss'],'validation_accuracy':score['accuracy'],'validation_macro_f1':score['macro_f1']})
        print(f"{artifact['algorithm']} {parameters}: validation log loss={score['log_loss']:.6f}",flush=True)
        if score['log_loss'] < best_loss:
            best, best_loss, best_model = artifact, score['log_loss'], model
    for k in (5,15,31,61):
        artifact = copy.deepcopy(baseline)
        artifact.update(algorithm='knn_classifier',reference_vectors=xs['train'].tolist(),reference_labels=ys['train'].tolist(),class_prior=prior.tolist(),k=k,smoothing=3.0,temperature=1.0)
        consider(artifact,{'k':k,'smoothing':3.0})
    spark = (SparkSession.builder.appName('MatchOutcomeAlgorithmComparison').master(os.getenv('SPARK_MASTER','local[2]'))
             .config('spark.driver.memory','512m').config('spark.hadoop.fs.defaultFS','file:///')
             .config('spark.hadoop.dfs.blocksize','67108864').config('spark.hadoop.parquet.block.size','67108864').config('spark.hadoop.dfs.replication','2')
             .config('spark.sql.shuffle.partitions','4').config('spark.ui.enabled','false').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        training = spark.createDataFrame([(float(y),Vectors.dense(x),float(1/(3*prior[y]))) for y,x in zip(ys['train'],xs['train'])],['label','features','class_weight'])
        for depth in (3,5,7):
            for balanced in (False,True):
                params = {'num_trees':64,'max_depth':depth,'min_instances':8,'balanced':balanced,'seed':2026}
                estimator = RandomForestClassifier(numTrees=64,maxDepth=depth,minInstancesPerNode=8,maxMemoryInMB=64,featureSubsetStrategy='sqrt',seed=2026)
                if balanced:
                    estimator = estimator.setWeightCol('class_weight')
                model = estimator.fit(training)
                artifact = copy.deepcopy(baseline)
                artifact.update(algorithm='random_forest',trees=[export_tree(t._java_obj.rootNode()) for t in model.trees],hyperparameters=params,temperature=1.0)
                consider(artifact,params,model)
        # Freeze selection before touching the held-out season.
        calibration_logits = np.stack([base_logits(x,best) for x in xs['calibration']])
        temperatures = np.unique(np.r_[np.geomspace(.05,10,401),1.0])
        losses = [metrics(ys['calibration'],softmax(calibration_logits/t))['log_loss'] for t in temperatures]
        best['temperature'] = float(temperatures[int(np.argmin(losses))])
        recipe = {'algorithm':best['algorithm'],'parameters':best.get('hyperparameters',{'k':best.get('k'),'reg_param':best.get('reg_param')}),'recipe':'comparison-v1'}
        best['model_version'] = 'wdl_'+hashlib.sha256((best['dataset_fingerprint']+json.dumps(recipe,sort_keys=True)).encode()).hexdigest()[:20]
        all_x = np.stack([vector(r,best)[0] for r in rows])
        parity_input = spark.createDataFrame([(i,Vectors.dense(x)) for i,x in enumerate(all_x)],['index','features'])
        baseline_dir = output/'logistic_baseline'
        if fresh_baseline or not (baseline_dir/'model.json').exists():
            LogisticRegressionModel.load((output/'spark_model').resolve().as_uri()).write().overwrite().save((baseline_dir/'spark_model').resolve().as_uri())
            baseline_dir.mkdir(exist_ok=True)
            for name in ('model.json','metrics.json'):
                shutil.copy2(output/name,baseline_dir/name)
        if best['algorithm'] == 'knn_classifier':
            expected = softmax(np.log(np.clip(spark_knn_probabilities(spark,all_x,best),1e-15,1))/best['temperature'])
            spark.createDataFrame([(i,int(y),list(map(float,x))) for i,(y,x) in enumerate(zip(ys['train'],xs['train']))], 'reference_id int, label int, features array<double>').write.mode('overwrite').parquet((output/'knn_reference').resolve().as_uri())
        else:
            model = best_model or LogisticRegressionModel.load((baseline_dir/'spark_model').resolve().as_uri())
            observed = model.transform(parity_input).select('index','rawPrediction' if best['algorithm']=='logistic_regression' else 'probability').collect()
            expected = np.zeros((len(rows),3))
            for r in observed:
                logits = r[1].toArray() if best['algorithm']=='logistic_regression' else np.log(np.clip(r[1].toArray(),1e-15,1))
                expected[r['index']] = softmax(logits/best['temperature'])
            model.write().overwrite().save((output/'spark_model').resolve().as_uri())
        actual = np.asarray([list(predict(r,best)['probabilities'].values()) for r in rows])
        error = float(np.max(np.abs(expected-actual)))
        if error > 1e-8:
            raise RuntimeError('Spark/NumPy parity failed: '+str(error))
    finally:
        spark.stop()
    test_p = softmax(np.stack([base_logits(x,best) for x in xs['test']])/best['temperature'])
    report.update(model_version=best['model_version'],algorithm=best['algorithm'],temperature=best['temperature'],test=metrics(ys['test'],test_p),
                  spark_numpy_max_absolute_error=error,comparison=candidates,selection_rule='minimum first-half 2023-2024 validation log loss; test untouched',selected_validation_log_loss=best_loss)
    if best['algorithm'] != 'logistic_regression':
        for key in ('coefficients','intercepts','reg_param'):
            best.pop(key,None)
        report['logistic_baseline_reg_param'] = report.pop('reg_param',None)
    for name,value in [('model.json',best),('metrics.json',report)]:
        temporary=output/(name+'.tmp')
        temporary.write_text(json.dumps(value,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8')
        temporary.replace(output/name)
        version=output/'versions'/best['model_version']
        version.mkdir(parents=True,exist_ok=True)
        shutil.copy2(output/name,version/name)
    with (output/'predictions.csv').open('w',encoding='utf-8-sig',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=['match_id','season','actual',*LABELS,'model_version'])
        writer.writeheader()
        for r in rows:
            writer.writerow({'match_id':r['match_id'],'season':r['season'],'actual':LABELS[r['label']],**predict(r,best)['probabilities'],'model_version':best['model_version']})
    write_report(output,report)
    print(json.dumps({'selected':best['algorithm'],'test':report['test'],'parity':error}),flush=True)
    return report

if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--output',default=str(Path(__file__).parent/'outputs'))
    args=parser.parse_args()
    compare(args.output)
