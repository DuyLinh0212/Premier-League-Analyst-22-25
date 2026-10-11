import unittest
from unittest.mock import Mock, patch

from analytics import api, neon_store


class NeonServingCatalogTests(unittest.TestCase):
    def test_catalog_uses_neon_metadata(self):
        rows = [{"name": "matches", "row_count": 1140, "metadata": {"storage": "neon"}}]
        with patch.object(neon_store, "catalog", return_value=rows) as catalog:
            self.assertEqual(api.catalog(), rows)
        catalog.assert_called_once_with()

    def test_data_rows_uses_published_neon_report(self):
        with patch.object(neon_store, "table_rows", return_value=[{"match_id": "m-1"}]) as table_rows:
            self.assertEqual(api.data_rows("matches", offset=20, limit=10), [{"match_id": "m-1"}])
        table_rows.assert_called_once_with("matches", offset=20, limit=10)

    def test_schema_ddl_runs_once_per_api_process(self):
        connection = Mock()
        connection.__enter__ = Mock(return_value=connection)
        connection.__exit__ = Mock(return_value=False)
        cursor = Mock()
        cursor.__enter__ = Mock(return_value=cursor)
        cursor.__exit__ = Mock(return_value=False)
        connection.cursor.return_value = cursor
        with patch.object(neon_store, "_schema_ready", False), patch.object(neon_store, "_connect", return_value=connection) as connect:
            neon_store.schema()
            neon_store.schema()
        connect.assert_called_once_with()
        self.assertGreater(cursor.execute.call_count, 0)


if __name__ == "__main__":
    unittest.main()
