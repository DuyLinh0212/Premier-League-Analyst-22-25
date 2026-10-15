import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from analytics.publish import SEASONS, _publish_with_reader


class PublishWithoutKnnTests(unittest.TestCase):
    def test_other_reports_publish_without_knn_directory(self):
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder)
            for filename in ("model.json", "metrics.json", "forecast_v2.json", "forecast_v2_metrics.json"):
                (output / filename).write_text(json.dumps({"model_version": "test"}), encoding="utf-8")
            (output / "matches.jsonl").write_text("", encoding="utf-8")
            teams = [dict(season=season, club_id=str(i), rank=i+1, goals_for=40,
                          goals_against=30, matches_played=38, wins=10, draws=10,
                          losses=18, points=40) for season in SEASONS for i in range(20)]
            def reader(table):
                return teams if table == "facts/fact_team_season" else []
            with patch.dict(os.environ, MATCH_MODEL_DIR=str(output), KNN_OUTPUT_DIR=str(output / "missing-knn")), \
                 patch("analytics.publish.FootballData", return_value=SimpleNamespace(clubs={})), \
                 patch("analytics.publish.evaluate", return_value={}), \
                 patch("analytics.publish.phases", return_value={}), \
                 patch("analytics.identity.link_players", return_value=[]), \
                 patch("analytics.publish.neon_store.run_complete", return_value=False):
                pending = output / "pending.json"
                _publish_with_reader("test-dataset", reader, "test-run", defer_neon=True, pending_path=pending)
            reports = json.loads(pending.read_text(encoding="utf-8"))
            names = [report[0] for report in reports]
            self.assertEqual(len(reports), 21)
            self.assertFalse(any(name.startswith("knn") for name in names))
            self.assertEqual(names.count("actual_standings"), 3)
            for name in ("outlook", "phases", "model", "forecast_v2", "matches", "players"):
                self.assertIn(name, names)
