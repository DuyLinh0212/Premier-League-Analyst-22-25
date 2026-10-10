"""FastAPI endpoint that returns a deterministic simulated league table."""

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import PlainTextResponse, Response

from league_table import generate_csv


app = FastAPI(title="Simulated Premier League Table API", version="1.0.0")


@app.get("/health", response_class=PlainTextResponse)
def health() -> str:
    return "ok"


@app.get("/league-table.csv")
def league_table_csv(season: str = Query(..., examples=["2025-2026"])) -> Response:
    try:
        content = generate_csv(season)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return Response(
        content=content.encode("utf-8"),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="league_table.csv"'},
    )
