from __future__ import annotations

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from .models import DashboardSummary, HealthResponse, TrendResponse
from .services import get_dashboard_summary, get_heart_rate_trend


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


@app.get("/health", response_model=HealthResponse)
def healthcheck() -> HealthResponse:
    return HealthResponse(status="ok")


@app.get("/api/v1/dashboard", response_model=DashboardSummary)
def dashboard_summary() -> DashboardSummary:
    return get_dashboard_summary()


@app.get("/api/v1/heart-rate", response_model=TrendResponse)
def heart_rate_trend(
    limit: int = Query(default=100, ge=1, le=1000),
) -> TrendResponse:
    return get_heart_rate_trend(limit=limit)
