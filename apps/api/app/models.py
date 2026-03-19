from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str


class SummaryMetric(BaseModel):
    label: str
    value: float | int | None
    unit: str | None = None
    recorded_at: datetime | None = None


class SleepSummary(BaseModel):
    sleep_id: str
    start: datetime
    end: datetime
    score: float | None
    avg_bpm: int
    avg_hrv: int
    duration_hours: float


class DashboardSummary(BaseModel):
    last_updated: datetime | None
    heart_rate: SummaryMetric
    hrv: SummaryMetric
    stress: SummaryMetric
    spo2: SummaryMetric
    skin_temp: SummaryMetric
    latest_sleep: SleepSummary | None
    steps: SummaryMetric


class HeartRatePoint(BaseModel):
    time: datetime
    bpm: int
    stress: float | None = None
    spo2: float | None = None
    skin_temp: float | None = None


class TrendResponse(BaseModel):
    points: list[HeartRatePoint]
