import pandas as pd
import pandas_market_calendars as mcal

_NYSE = mcal.get_calendar("NYSE")

def get_sessions(start, end) -> pd.DataFrame:
    """One row per trading day: market_open, market_close (tz-aware UTC)."""
    return _NYSE.schedule(start_date=start, end_date=end)

def trading_days(start, end) -> list[pd.Timestamp]:
    return list(get_sessions(start, end).index)

def session_minutes(start, end) -> pd.DatetimeIndex:
    """Every 1-min bar timestamp inside regular sessions."""
    return mcal.date_range(get_sessions(start, end), frequency="1min", closed="left")

def is_market_open(ts: pd.Timestamp | None = None) -> bool:
    ts = ts or pd.Timestamp.now(tz="UTC")
    sessions = get_sessions(ts.normalize(), ts.normalize())
    return _NYSE.open_at_time(sessions, ts) if not sessions.empty else False
