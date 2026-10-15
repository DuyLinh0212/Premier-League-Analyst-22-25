import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
from match_outcome.api import app


class MatchesEndpointTests(unittest.TestCase):
    def test_history_is_available_without_loading_prediction_model(self):
        row = {"match_id": "match-1", "season": "2024-2025", "home_team": "ARS",
               "away_team": "CHE", "home_stats": {"shots": 10},
               "away_stats": {"shots": 8}}
        with patch("match_outcome.api.get_report", side_effect=AssertionError("Model must not be loaded")), \
             patch("analytics.neon_store.match_examples", return_value=[row]) as report, \
             TestClient(app) as client:
            response = client.get("/matches")
        self.assertEqual(response.status_code, 200)
        report.assert_called_once_with()
        self.assertEqual(response.json()[0]["home_stats"], {"shots": 10})
        self.assertEqual(response.json(), [row])

    def test_missing_published_history_returns_404(self):
        with patch("analytics.neon_store.match_examples", side_effect=LookupError("No published matches")), TestClient(app) as client:
            response = client.get("/matches")
        self.assertEqual(response.status_code, 404)
