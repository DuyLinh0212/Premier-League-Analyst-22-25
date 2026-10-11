import unittest
from unittest.mock import patch

from psycopg.types.json import Jsonb

from analytics import neon_store


class _Cursor:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []
        self.last_sql = ""

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, sql, params=None):
        self.last_sql = sql
        self.calls.append((sql, params))

    def fetchone(self):
        if "SELECT status FROM pl_analysis_runs" in self.last_sql:
            return ("processing",)
        if "SELECT payload FROM pl_analysis_reports" in self.last_sql:
            return (self.payload,)
        if "SELECT count(*) FROM pl_analysis_reports" in self.last_sql:
            return (1,)
        raise AssertionError("Unexpected fetchone query: " + self.last_sql)


class _Connection:
    def __init__(self, cursor):
        self._cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def cursor(self):
        return self._cursor


class NeonJsonbBindingTests(unittest.TestCase):
    def test_report_filters_and_payload_are_bound_as_jsonb(self):
        filters = {"season": "2024-2025"}
        payload = {"model_version": "test-model", "scores": [{"home_win": 0.5}]}
        cursor = _Cursor(payload)
        connection = _Connection(cursor)

        with patch.object(neon_store, "begin_run"), patch.object(neon_store, "_connect", return_value=connection):
            neon_store.publish_reports("run-1", "dataset-1", [("forecast", payload, filters)])

        insert = next(params for sql, params in cursor.calls if sql.startswith("INSERT INTO pl_analysis_reports"))
        self.assertIsInstance(insert[3], Jsonb)
        self.assertIsInstance(insert[4], Jsonb)
        self.assertEqual(insert[3].obj, filters)
        self.assertEqual(insert[4].obj, payload)


if __name__ == "__main__":
    unittest.main()
