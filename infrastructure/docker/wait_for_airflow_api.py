"""Wait until the Airflow 3 Execution API is ready for task runners."""

from __future__ import annotations

import time
from urllib.request import urlopen


API_HEALTH_URL = "http://airflow-api-server:8080/api/v2/monitor/health"


def main() -> None:
    last_error: Exception | None = None
    for _ in range(60):
        try:
            with urlopen(API_HEALTH_URL, timeout=3) as response:
                if response.status == 200:
                    print("Airflow Execution API is ready")
                    return
        except Exception as error:
            last_error = error
            time.sleep(2)
    raise SystemExit(f"Airflow Execution API did not become ready: {last_error}")


if __name__ == "__main__":
    main()
