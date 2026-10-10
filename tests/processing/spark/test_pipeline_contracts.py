import sys
import unittest
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "processing/spark"))
from pl_pipeline.config import CLUBS
from pl_pipeline.core import Context, normalize, clean_json


class Contracts(unittest.TestCase):
    def test_accent_and_punctuation(self):
        self.assertEqual(normalize("João  Pedro"), "joao pedro")
        self.assertEqual(normalize("Brighton_&_Hove_Albion"), "brighton and hove albion")

    def test_aliases_do_not_overlap_clubs(self):
        seen = {}
        for code, name, tm, aliases in CLUBS:
            for alias in [name, *aliases]:
                key = normalize(alias)
                self.assertIn(seen.get(key, code), [code])
                seen[key] = code

    def test_canonical_ids_are_unique(self):
        self.assertEqual(len({c[0] for c in CLUBS}), 24)
        self.assertEqual(len({c[2] for c in CLUBS}), 24)


    def test_no_substring_matching(self):
        aliases = {normalize(a) for c in CLUBS for a in [c[1], *c[3]]}
        self.assertNotIn("arsenal women", aliases)
        self.assertNotIn("manchester", aliases)



    def test_sql_decimals_export_as_numbers(self):
        self.assertEqual(clean_json({"coverage_pct": Decimal("86.66667")}), {"coverage_pct": 86.66667})

    def test_invalid_run_ids_rejected(self):
        for value in ["../old", "a/b", "x'; rm", "", "a"*101]:
            with self.assertRaises(ValueError):
                Context(value)

    def test_player_id_is_not_last_column(self):
        source = (ROOT / "processing/spark/pl_pipeline/identity.py").read_text()
        self.assertIn("sha2(concat(name_key", source)
        self.assertNotIn("columns[-1]", source)



if __name__ == "__main__":
    unittest.main(verbosity=2)
