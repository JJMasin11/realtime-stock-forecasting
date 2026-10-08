"""
Phase 1 of the historical backfill: fetch raw 1-min OHLCV bars from Twelve Data.

Writes one Parquet file per (symbol, 12-session chunk) to
data/raw/symbol=<SYM>/<first_session>_<last_session>.parquet.
File existence is the checkpoint, so reruns skip finished chunks.
No Spark here; historical_backfill.py reads these files in phase 2.
"""

import logging
import os
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from twelvedata.exceptions import BadRequestError, InvalidApiKeyError, TwelveDataError

import realtime_stock_forecasting.utils.logger  # noqa: F401  (configures logging)
from realtime_stock_forecasting.backfill.twelve_data_client import (
    MAX_OUTPUTSIZE,
    TwelveDataClient,
)
from realtime_stock_forecasting.ingestion.market_calendar import get_sessions
from realtime_stock_forecasting.utils.config import settings

logger = logging.getLogger(__name__)

INTERVAL = "1min"
SESSIONS_PER_CHUNK = 12  # 12 x 390 = 4,680 bars < 5,000 cap
CALLS_PER_MINUTE = 8  # free tier
DAILY_CALL_BUDGET = 790  # free tier is 800/day; leave headroom
MAX_RETRIES = 5
TS_FORMAT = "%Y-%m-%d %H:%M:%S"
RAW_SCHEMA = {
    "open": "float64",
    "high": "float64",
    "low": "float64",
    "close": "float64",
    "volume": "int64",
}


class CallBudgetExhausted(RuntimeError):
    """Raised when this run has used its daily API call budget."""


class RateLimiter:
    """
    Spaces API calls to stay under the per-minute limit and stops the run at the daily budget.

    Args:
        calls_per_minute (int): Max calls per minute allowed by the plan.
        max_calls (int): Max calls this run may make before raising CallBudgetExhausted.
    """

    def __init__(self, calls_per_minute: int, max_calls: int):
        self._min_interval = 60 / calls_per_minute
        self._max_calls = max_calls
        self._last_call = 0.0
        self.calls = 0

    def wait(self) -> None:
        if self.calls >= self._max_calls:
            raise CallBudgetExhausted(f"Used {self.calls} API calls this run")
        elapsed = time.monotonic() - self._last_call
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_call = time.monotonic()
        self.calls += 1


def call_with_retry(fn, limiter: RateLimiter, *args, **kwargs):
    """
    Calls `fn` through the rate limiter, backing off on transient errors.

    BadRequestError (e.g. no data for the range) and InvalidApiKeyError are not retried.
    Rate-limit (429), 5xx and network errors are retried up to MAX_RETRIES times.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        limiter.wait()
        try:
            return fn(*args, **kwargs)
        except (BadRequestError, InvalidApiKeyError):
            raise
        except (TwelveDataError, requests.RequestException) as e:
            if attempt == MAX_RETRIES:
                raise
            backoff = min(15 * 2 ** (attempt - 1), 120)
            logger.warning(
                "Attempt %d/%d failed (%s); retrying in %ds", attempt, MAX_RETRIES, e, backoff
            )
            time.sleep(backoff)


def last_completed_session() -> pd.Timestamp:
    """Date of the most recent session that has already closed (never today's live session)."""
    now = pd.Timestamp.now(tz="UTC")
    sessions = get_sessions(now - pd.Timedelta(days=10), now)
    closed = sessions[sessions["market_close"] <= now]
    return closed.index[-1]


def build_chunks(
    start, end, sessions_per_chunk: int = SESSIONS_PER_CHUNK
) -> list[tuple[pd.Timestamp, pd.Timestamp, str]]:
    """
    Splits the trading sessions in [start, end] into runs of `sessions_per_chunk`.

    Args:
        start: First date to include.
        end: Last date to include.
        sessions_per_chunk (int): Sessions per API call; must keep bars under the 5,000 cap.

    Returns:
        list[tuple]: (first market_open, last market_close, file stem) per chunk, where the
        stem is 'YYYY-MM-DD_YYYY-MM-DD' (no colons, so it's a valid Windows filename).
    """
    sessions = get_sessions(start, end)
    chunks = []
    for _, group in sessions.groupby(np.arange(len(sessions)) // sessions_per_chunk):
        stem = f"{group.index[0]:%Y-%m-%d}_{group.index[-1]:%Y-%m-%d}"
        chunks.append((group["market_open"].iloc[0], group["market_close"].iloc[-1], stem))
    return chunks


def fetch_chunk(
    client: TwelveDataClient,
    limiter: RateLimiter,
    symbol: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    """Fetches one chunk of bars; returns an empty frame if Twelve Data has no data for it."""
    try:
        pdf = call_with_retry(
            client.get_time_series,
            limiter,
            symbol=symbol,
            interval=INTERVAL,
            start=start.tz_convert("UTC").strftime(TS_FORMAT),
            end=end.tz_convert("UTC").strftime(TS_FORMAT),
        )
    except BadRequestError as e:
        logger.warning("%s %s..%s: no data (%s)", symbol, start, end, e)
        return pd.DataFrame()

    if len(pdf) >= MAX_OUTPUTSIZE:
        logger.warning(
            "%s %s..%s hit the %d-row cap; oldest bars may be missing",
            symbol, start, end, MAX_OUTPUTSIZE,
        )
    return pdf


def clean(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Normalises a raw Twelve Data frame to the raw-file schema.

    `datetime` is the index (newest first) on the way in. On the way out: event_timestamp
    (naive UTC, microsecond precision) plus RAW_SCHEMA columns, sorted ascending. No symbol
    column, since the symbol=<SYM> folder carries it.
    """
    pdf = pdf.reset_index().rename(columns={"datetime": "event_timestamp"})
    pdf["event_timestamp"] = pd.to_datetime(pdf["event_timestamp"]).astype("datetime64[us]")
    pdf = pdf[["event_timestamp", *RAW_SCHEMA]].astype(RAW_SCHEMA)
    return pdf.sort_values("event_timestamp").reset_index(drop=True)


def write_chunk(pdf: pd.DataFrame, out_path: Path) -> None:
    """
    Writes atomically: temp file first, then rename.

    The temp name starts with '_' so Spark ignores it if a crash leaves it behind.
    """
    tmp_path = out_path.with_name(f"_{out_path.name}.tmp")
    pdf.to_parquet(
        tmp_path,
        engine="pyarrow",
        index=False,
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )
    os.replace(tmp_path, out_path)


def fetch_symbol(
    client: TwelveDataClient,
    limiter: RateLimiter,
    symbol: str,
    end: pd.Timestamp,
    raw_dir: Path,
) -> None:
    """Fetches every missing chunk for one symbol, from its earliest bar up to `end`."""
    earliest = call_with_retry(
        client.get_earliest_timestamp, limiter, symbol=symbol, interval=INTERVAL
    )
    chunks = build_chunks(earliest.normalize(), end)
    logger.info("%s: earliest %s, %d chunks", symbol, earliest, len(chunks))

    out_dir = raw_dir / f"symbol={symbol}"
    out_dir.mkdir(parents=True, exist_ok=True)

    for start, stop, stem in chunks:
        out_path = out_dir / f"{stem}.parquet"
        if out_path.exists():
            logger.debug("%s %s: already fetched, skipping", symbol, stem)
            continue

        pdf = fetch_chunk(client, limiter, symbol, start, stop)
        if pdf.empty:
            continue  # not written, so the next run retries it

        write_chunk(clean(pdf), out_path)
        logger.info("%s %s: wrote %d bars", symbol, stem, len(pdf))


def main() -> None:
    logging.getLogger().addHandler(logging.StreamHandler())  # progress on the console too

    client = TwelveDataClient(api_key=settings.twelve_data_api_key)
    limiter = RateLimiter(CALLS_PER_MINUTE, DAILY_CALL_BUDGET)
    end = last_completed_session()
    raw_dir = settings.data_dir / "raw"
    logger.info("Fetching %s up to %s into %s", settings.tickers, end.date(), raw_dir)

    for symbol in settings.tickers:
        try:
            fetch_symbol(client, limiter, symbol, end, raw_dir)
        except CallBudgetExhausted:
            logger.warning("Daily call budget reached; rerun tomorrow to continue")
            break
        except Exception:
            logger.exception("%s: failed, moving on to the next ticker", symbol)

    logger.info("Done: %d API calls used", limiter.calls)


if __name__ == "__main__":
    main()
