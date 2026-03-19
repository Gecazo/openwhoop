from __future__ import annotations

from datetime import datetime

from .db import get_connection
from .models import DashboardSummary, HeartRatePoint, SleepSummary, SummaryMetric, TrendResponse


def _parse_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None

    for candidate in (value, value.replace(" ", "T")):
        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            continue

    return None


def get_dashboard_summary() -> DashboardSummary:
    with get_connection() as connection:
        latest_row = connection.execute(
            """
            SELECT time, bpm, stress, spo2, skin_temp
            FROM heart_rate
            ORDER BY time DESC
            LIMIT 1
            """
        ).fetchone()

        hrv_row = connection.execute(
            """
            SELECT end, avg_hrv
            FROM sleep_cycles
            ORDER BY end DESC
            LIMIT 1
            """
        ).fetchone()

        sleep_row = connection.execute(
            """
            SELECT sleep_id, start, end, score, avg_bpm, avg_hrv
            FROM sleep_cycles
            ORDER BY end DESC
            LIMIT 1
            """
        ).fetchone()

        today_steps_row = connection.execute(
            """
            SELECT COUNT(*) AS steps
            FROM heart_rate
            WHERE date(time) = date('now', 'localtime')
            """
        ).fetchone()

    last_updated = _parse_datetime(latest_row["time"]) if latest_row else None

    latest_sleep = None
    if sleep_row:
        start = _parse_datetime(sleep_row["start"])
        end = _parse_datetime(sleep_row["end"])
        duration_hours = ((end - start).total_seconds() / 3600) if start and end else 0.0

        latest_sleep = SleepSummary(
            sleep_id=sleep_row["sleep_id"],
            start=start,
            end=end,
            score=sleep_row["score"],
            avg_bpm=sleep_row["avg_bpm"],
            avg_hrv=sleep_row["avg_hrv"],
            duration_hours=round(duration_hours, 2),
        )

    return DashboardSummary(
        last_updated=last_updated,
        heart_rate=SummaryMetric(
            label="Heart Rate",
            value=latest_row["bpm"] if latest_row else None,
            unit="bpm",
            recorded_at=last_updated,
        ),
        hrv=SummaryMetric(
            label="HRV",
            value=hrv_row["avg_hrv"] if hrv_row else None,
            unit="ms",
            recorded_at=_parse_datetime(hrv_row["end"]) if hrv_row else None,
        ),
        stress=SummaryMetric(
            label="Stress",
            value=latest_row["stress"] if latest_row else None,
            unit=None,
            recorded_at=last_updated,
        ),
        spo2=SummaryMetric(
            label="SpO2",
            value=latest_row["spo2"] if latest_row else None,
            unit="%",
            recorded_at=last_updated,
        ),
        skin_temp=SummaryMetric(
            label="Skin Temp",
            value=latest_row["skin_temp"] if latest_row else None,
            unit="C",
            recorded_at=last_updated,
        ),
        latest_sleep=latest_sleep,
        steps=SummaryMetric(
            label="Steps",
            value=today_steps_row["steps"] if today_steps_row else 0,
            unit="samples",
            recorded_at=last_updated,
        ),
    )


def get_heart_rate_trend(limit: int = 100) -> TrendResponse:
    query_limit = max(1, min(limit, 1000))

    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT time, bpm, stress, spo2, skin_temp
            FROM heart_rate
            ORDER BY time DESC
            LIMIT ?
            """,
            (query_limit,),
        ).fetchall()

    points = [
        HeartRatePoint(
            time=_parse_datetime(row["time"]),
            bpm=row["bpm"],
            stress=row["stress"],
            spo2=row["spo2"],
            skin_temp=row["skin_temp"],
        )
        for row in reversed(rows)
    ]

    return TrendResponse(points=points)
