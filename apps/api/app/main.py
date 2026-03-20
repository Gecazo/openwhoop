from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

from .models import (
    DashboardSummary,
    HealthResponse,
    SkinTemperatureTrendResponse,
    SyncResponse,
    TrendResponse,
)
from .services import get_dashboard_summary, get_heart_rate_trend, get_skin_temperature_trend


app = FastAPI(
    title="OpenWhoop API",
    version="0.1.0",
    description="Python API for serving OpenWhoop stats to web and mobile clients.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

WEB_INDEX = Path(__file__).resolve().parents[2] / "web" / "index.html"


@app.get("/", include_in_schema=False)
def web_dashboard() -> FileResponse:
    return FileResponse(WEB_INDEX)


@app.get("/health", response_model=HealthResponse)
def healthcheck() -> HealthResponse:
    return HealthResponse(status="ok")


@app.post("/api/v1/sync", response_model=SyncResponse)
def sync_data() -> SyncResponse:
    return SyncResponse(
        status="manual",
        message="Run the local OpenWhoop sync command in your terminal, then refresh the dashboard.",
        command="cargo run -r -- download-history",
    )


@app.get("/api/v1/dashboard", response_model=DashboardSummary)
def dashboard_summary() -> DashboardSummary:
    return get_dashboard_summary()


@app.get("/api/v1/heart-rate", response_model=TrendResponse)
def heart_rate_trend(
    hours: int = Query(default=12, ge=1, le=72),
) -> TrendResponse:
    return get_heart_rate_trend(hours=hours)


@app.get("/api/v1/skin-temperature", response_model=SkinTemperatureTrendResponse)
def skin_temperature_trend(
    hours: int = Query(default=12, ge=1, le=72),
) -> SkinTemperatureTrendResponse:
    return get_skin_temperature_trend(hours=hours)
