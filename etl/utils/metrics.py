"""API call-metrics capture (Section 5, observability transform).

``fetch_json`` is the single HTTP entry point for every extract function. It
performs the request with retries/backoff, times it, parses rate-limit headroom
from response headers where the source exposes it (NASA's X-RateLimit-Remaining),
and returns the parsed JSON alongside a CallMetrics record.

This runs independently of the domain transforms — it's metadata *about* the
call, not the data — and never blocks the main pipeline. The metrics feed
open_sky_logs.api_call_log via etl.load.postgres_loader."""
from __future__ import annotations

import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

import requests

from etl.config import HTTP_TIMEOUT_S, RETRY_ATTEMPTS
from etl.utils.logging import get_logger
from etl.utils.retry import backoff_delay

log = get_logger("etl.metrics")

# Column order matches open_sky_logs.api_call_log (minus the bigserial id).
API_CALL_LOG_COLUMNS = [
    "source_name", "called_at", "response_time_ms", "http_status_code",
    "success", "retry_count", "records_returned", "rate_limit_remaining",
    "pipeline_run_id",
]


@dataclass
class CallMetrics:
    source_name: str
    called_at: str  # ISO8601 UTC
    response_time_ms: int | None = None
    http_status_code: int | None = None
    success: bool = False
    retry_count: int = 0
    records_returned: int | None = None
    rate_limit_remaining: int | None = None
    pipeline_run_id: str | None = None

    def as_row(self) -> dict[str, Any]:
        d = asdict(self)
        return {k: d[k] for k in API_CALL_LOG_COLUMNS}


def _parse_rate_limit_remaining(headers: requests.structures.CaseInsensitiveDict) -> int | None:
    # NASA exposes X-RateLimit-Remaining; Open-Meteo exposes none.
    for key in ("X-RateLimit-Remaining", "x-ratelimit-remaining"):
        if key in headers:
            try:
                return int(headers[key])
            except (TypeError, ValueError):
                return None
    return None


def fetch_json(
    source_name: str,
    url: str,
    params: dict[str, Any],
    *,
    pipeline_run_id: str | None = None,
    attempts: int = RETRY_ATTEMPTS,
    timeout: int = HTTP_TIMEOUT_S,
) -> tuple[Any, CallMetrics]:
    """GET ``url`` with retries; return (parsed_json, CallMetrics).

    ``records_returned`` is left None here (the caller knows the response shape
    and fills it in) — everything else is captured automatically."""
    metrics = CallMetrics(
        source_name=source_name,
        called_at=datetime.now(timezone.utc).isoformat(),
        pipeline_run_id=pipeline_run_id,
    )
    started = time.monotonic()
    last_exc: Exception | None = None

    for attempt in range(attempts):
        metrics.retry_count = attempt
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            metrics.http_status_code = resp.status_code
            metrics.rate_limit_remaining = _parse_rate_limit_remaining(resp.headers)
            resp.raise_for_status()
            metrics.response_time_ms = int((time.monotonic() - started) * 1000)
            metrics.success = True
            log.info(
                "api call ok",
                extra={"context": {"source": source_name, "status": resp.status_code,
                                   "ms": metrics.response_time_ms, "retries": attempt,
                                   "rate_limit_remaining": metrics.rate_limit_remaining}},
            )
            return resp.json(), metrics
        except requests.RequestException as exc:
            last_exc = exc
            if hasattr(exc, "response") and exc.response is not None:
                metrics.http_status_code = exc.response.status_code
            if attempt < attempts - 1:
                time.sleep(backoff_delay(attempt))

    # Exhausted retries: record the failure and re-raise (extract task fails,
    # Airflow retries/alerts). Metrics are still returned via the exception path
    # by the caller wrapping this — but the domain fetch cannot proceed.
    metrics.response_time_ms = int((time.monotonic() - started) * 1000)
    metrics.success = False
    log.error(
        "api call failed after retries",
        extra={"context": {"source": source_name, "attempts": attempts,
                           "status": metrics.http_status_code, "error": str(last_exc)}},
    )
    assert last_exc is not None
    raise CallFailed(str(last_exc), metrics) from last_exc


class CallFailed(Exception):
    """Raised when an API call fails after all retries. Carries the partial
    CallMetrics so the caller can still log the failed attempt to
    open_sky_logs.api_call_log before propagating the failure."""

    def __init__(self, message: str, metrics: CallMetrics):
        super().__init__(message)
        self.metrics = metrics
