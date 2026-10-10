"""Danh's Spark SQL, actual GMM and explicitly separate rule-based tiers."""
import json
import os
from datetime import date
from decimal import Decimal
from pathlib import Path


def json_rows(frame):
    def clean(value):
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, date):
            return value.isoformat()
        if hasattr(value, 'toArray'):
            return [float(x) for x in value.toArray()]
        return value
    return [{k: clean(v) for k, v in row.asDict().items()} for row in frame.collect()]


def analyze(reader, spark=None):
    from pyspark.sql import SparkSession, functions as F
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType
    from pyspark.sql.window import Window
    from pyspark.ml.feature import VectorAssembler, StandardScaler
    from pyspark.ml.clustering import GaussianMixture
    owned = spark is None
    spark = spark or (SparkSession.builder.appName('PremierLeagueMergedAnalytics').master(os.getenv('SPARK_MASTER', 'local[2]'))
                     .config('spark.driver.memory', '512m').config('spark.ui.enabled', 'false')
                     .config('spark.sql.shuffle.partitions', '4').config('spark.sql.ansi.enabled', 'false').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    try:
        def frame(table, numeric=(), boolean=()):
            rows = reader(table)
            columns = list(rows[0])
            schema = StructType([StructField(c, DoubleType() if c in numeric else BooleanType() if c in boolean else StringType(), True) for c in columns])
            records = [{c: None if r[c] is None else (float(r[c]) if c in numeric else bool(r[c]) if c in boolean else str(r[c])) for c in columns} for r in rows]
            return spark.createDataFrame(records, schema)
        transfers = frame('facts/fact_transfer', ('fee_eur', 'listed_market_value_eur'), ('from_active_pl_season', 'to_active_pl_season'))
        clubs = frame('dimensions/dim_club')
        teams = frame('facts/fact_team_season', ('rank', 'points', 'wins', 'draws', 'losses'))
        transfers = (transfers.withColumn('to_pl_club', F.when(F.col('to_active_pl_season'), F.col('to_club_id')))
                     .withColumn('from_pl_club', F.when(F.col('from_active_pl_season'), F.col('from_club_id')))
                     .withColumn('transfer_type', F.col('fee_status')).withColumn('market_value_eur', F.col('listed_market_value_eur'))
                     .withColumn('transfer_date', F.to_date('transfer_date')))
        if 'club_name' not in teams.columns:
            teams = teams.join(clubs.select('club_id', 'club_name'), 'club_id', 'left')
        for name, df in [('fact_transfer', transfers), ('dim_club', clubs), ('fact_team_season', teams)]:
            df.createOrReplaceTempView(name)
        queries = json.loads((Path(__file__).parent / 'transfer_queries.json').read_text(encoding='utf-8'))
        sql_reports = {name: json_rows(spark.sql(sql)) for name, sql in queries.items()}
        stats = transfers.filter(F.col('to_pl_club').isNotNull()).groupBy('to_pl_club').agg(
            F.count('*').alias('frequency'), F.sum('fee_eur').alias('monetary_eur'),
            F.max(F.when(F.col('fee_eur') > 0, F.col('transfer_date'))).alias('last_paid_transfer'))
        scored = (clubs.select('club_id', 'club_name').join(stats, F.col('club_id') == F.col('to_pl_club'), 'inner')
                  .withColumn('recency_days', F.coalesce(F.datediff(F.lit('2025-06-30'), F.col('last_paid_transfer')), F.lit(365)))
                  .fillna({'monetary_eur': 0.0, 'frequency': 0}).withColumn('monetary_mil_eur', F.round(F.col('monetary_eur') / 1e6, 2)))
        # Stable club-ID tiebreaks make repeat runs deterministic.
        for output, column, descending in [('r_score', 'recency_days', True), ('f_score', 'frequency', False), ('m_score', 'monetary_eur', False)]:
            scored = scored.withColumn(output, F.ntile(4).over(Window.orderBy(F.col(column).desc() if descending else F.col(column).asc(), 'club_id')))
        scored = scored.withColumn('rfm_code', F.concat_ws('-', 'r_score', 'f_score', 'm_score'))
        rules = scored.withColumn('tier_segment', F.when(F.col('monetary_mil_eur') >= 400, 'Tier 1')
                                  .when((F.col('monetary_mil_eur') >= 200) | (F.col('frequency') >= 45), 'Tier 2')
                                  .when(F.col('monetary_mil_eur') >= 100, 'Tier 3').otherwise('Tier 4'))
        ml = scored.withColumn('log_m', F.log1p('monetary_eur')).withColumn('log_f', F.log1p('frequency')).withColumn('r_val', F.col('recency_days').cast('double'))
        features = VectorAssembler(inputCols=['r_val', 'log_f', 'log_m'], outputCol='features').transform(ml)
        scaled = StandardScaler(inputCol='features', outputCol='scaled_features', withMean=True, withStd=True).fit(features).transform(features)
        model = GaussianMixture(featuresCol='scaled_features', predictionCol='cluster_id', k=4, seed=42, maxIter=50).fit(scaled)
        predicted = model.transform(scaled)
        profiles = json_rows(predicted.groupBy('cluster_id').agg(F.count('*').alias('club_count'), F.avg('monetary_mil_eur').alias('average_spend')).orderBy(F.col('average_spend').desc()))
        segments = {int(p['cluster_id']): 'Tier ' + str(i + 1) for i, p in enumerate(profiles)}
        selected = ['club_id', 'club_name', 'frequency', 'monetary_mil_eur', 'recency_days', 'rfm_code']
        gmm_rows = json_rows(predicted.select(*selected, 'cluster_id', 'probability').orderBy('club_id'))
        for row in gmm_rows:
            row['tier_segment'] = segments[row['cluster_id']]
        return {'sql': sql_reports, 'gmm': {'method': 'Spark MLlib GaussianMixture', 'seed': 42, 'k': 4, 'snapshot_date': '2025-06-30', 'profiles': profiles, 'clubs': gmm_rows},
                'rules': {'method': 'monetary/frequency thresholds', 'clubs': json_rows(rules.select(*selected, 'tier_segment').orderBy('club_id'))}}
    finally:
        if owned:
            spark.stop()
