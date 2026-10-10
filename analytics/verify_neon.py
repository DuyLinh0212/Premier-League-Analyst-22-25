"""CI gate: verify that a complete HDFS-derived report bundle is queryable."""
import argparse

from . import neon_store


def verify(expected_run=None, expected_reports=23):
    neon_store.schema()
    with neon_store._connect() as connection, connection.cursor() as cursor:
        if expected_run:
            cursor.execute("SELECT run_id,status,report_count FROM pl_analysis_runs WHERE run_id=%s", (expected_run,))
        else:
            cursor.execute("SELECT run_id,status,report_count FROM pl_analysis_runs WHERE dataset_id LIKE %s ORDER BY completed_at DESC NULLS LAST LIMIT 1", ("premier-league-22-25:silver:%",))
        run = cursor.fetchone()
        if not run or run[1] != "complete" or run[2] != expected_reports:
            raise RuntimeError(f"Neon publication gate failed: expected one complete run with {expected_reports} reports; observed {run}")
        cursor.execute("SELECT report_name,count(*) FROM pl_analysis_reports WHERE run_id=%s GROUP BY report_name ORDER BY report_name", (run[0],))
        report_kinds = cursor.fetchall()
        cursor.execute("SELECT count(*) FROM pl_analysis_active_reports a JOIN pl_analysis_reports p USING (run_id,report_name,filter_key) WHERE a.run_id=%s", (run[0],))
        active = cursor.fetchone()[0]
        if active != expected_reports:
            raise RuntimeError(f"Neon active pointer count mismatch: {active} != {expected_reports}")
    result = {"run_id": run[0], "status": run[1], "report_count": run[2], "active_report_count": active,
              "report_kinds": [{"name": name, "count": count} for name, count in report_kinds]}
    print(result, flush=True)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id")
    parser.add_argument("--expected-reports", type=int, default=23)
    args = parser.parse_args()
    verify(args.run_id, args.expected_reports)
