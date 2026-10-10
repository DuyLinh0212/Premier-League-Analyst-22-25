import json
import unittest
from pathlib import Path
from unittest.mock import patch
from analytics.ingest import ROOT, convert, silver_tables
from analytics.outlook.football_data import FootballData
from analytics.outlook.player_outlook import evaluate
from analytics.outlook.club_phases import analyze
from analytics.players import valuation, similar
from analytics.simulator import generate_simulated_transfers
from analytics.streaming import persist_events


class SourceContracts(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tables = {n: r for n, r, m in silver_tables()}

    def test_all_silver_rows_and_null_types(self):
        self.assertEqual(len(self.tables), 21)
        self.assertEqual(sum(map(len, self.tables.values())), 73075)
        self.assertEqual(len(self.tables['facts/fact_match_enriched']), 1140)
        self.assertEqual(len(self.tables['facts/fact_team_match']), 2280)
        self.assertEqual(len(self.tables['facts/fact_team_season']), 60)
        self.assertIsNone(convert('\\N', 'string'))
        self.assertEqual(convert('', 'string'), '')
        self.assertEqual(convert('0', 'integer'), 0)
        self.assertFalse(convert('false', 'boolean'))
        self.assertEqual(convert('0.000000000000000001', 'decimal(20,18)'), '0.000000000000000001')

    def test_data_matches_original_share_snapshot(self):
        provenance = json.loads((ROOT / 'data/integration_manifest.json').read_text(encoding='utf-8'))
        self.assertEqual(len(provenance['silver_overlap']), 7)
        self.assertTrue(all(r['identical_rows'] for r in provenance['silver_overlap']))

    def test_outlook_and_phase_totals(self):
        # Original reader expects CSV boolean/string conventions; analytics only uses numeric/date fields.
        data = FootballData(self.tables.__getitem__)
        phases = analyze(data)
        self.assertEqual(sum(len(v) for v in phases['seasons'].values()), 60)
        self.assertEqual(sum(len(r['stages']) for v in phases['seasons'].values() for r in v), 240)
        for rows in phases['seasons'].values():
            for club in rows:
                self.assertEqual([s['played'] for s in club['stages']], [10,9,10,9])
        for window in (1,2,3):
            report = evaluate(data, '2024-2025', window)
            self.assertEqual(len(report['players']), 562)
            self.assertEqual(len({p['player_id'] for p in report['players']}), 562)
        self.assertTrue(all(p['continuity'] is None for p in evaluate(data, '2022-2023')['players']))

    def test_datamb_records_preserved(self):
        rows = json.loads((ROOT / 'data/datamb_players_db.json').read_text(encoding='utf-8'))
        self.assertEqual(len(rows), 1350)
        self.assertEqual(len({p['id'] for p in rows}), 1337)
        from analytics.identity import link_players
        linked = link_players(rows,FootballData(self.tables.__getitem__))
        self.assertEqual(sum(r['canonical_player_id'] is not None for r in linked),1350)
        haaland = [r['canonical_player_id'] for r in linked if r['name']=='Erling Haaland']
        self.assertEqual(len(set(haaland)),1)

    def test_valuation_reference_and_zero(self):
        result = valuation(22, 'FW', 50, 20, 2500, 'tier2')
        self.assertEqual(result['estimated_fee_mil_eur'], 70.9)
        self.assertEqual(len(result['trajectory']), 3)
        self.assertEqual(valuation(22, 'FW', 0, 0, 0, 'tier2')['estimated_fee_mil_eur'], 0)

    def test_simulation_reproducible_and_isolated(self):
        self.assertEqual(generate_simulated_transfers(10, seed=42), generate_simulated_transfers(10, seed=42))
        self.assertTrue(all(r['season'] == '2025-2026' for r in generate_simulated_transfers(10)))

    def test_stream_failure_does_not_commit_database(self):
        event = {'event_id':'a'*32,'schema_version':1,'is_simulated':True,'season':'2025-2026'}
        with patch('analytics.store.query', return_value=[]) as database:
            def fail(event, checksum):
                raise OSError('HDFS disconnected')
            with self.assertRaises(OSError):
                persist_events([event], fail)
            self.assertEqual(database.call_count, 1)  # conflict read only, no commit
        with patch('analytics.store.query', return_value=[]):
            with self.assertRaises(ValueError):
                persist_events([event, {**event, 'season':'2026-2027'}], lambda e,h: None)


if __name__ == '__main__':
    unittest.main()
