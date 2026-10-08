import logging

import pandas as pd
from pyspark.sql import SparkSession

from realtime_stock_forecasting.feature_store.transformations import compute_features
from realtime_stock_forecasting.feature_store.writer import write_to_postgres
from realtime_stock_forecasting.ingestion.market_calendar import get_sessions
from realtime_stock_forecasting.utils.config import settings

logger = logging.getLogger(__name__)

# Sessions where fewer than this fraction of minutes have real bars are dropped, not filled
MIN_SESSION_COVERAGE = 0.5

def reindex_to_minute_grid(pdf: pd.DataFrame, start: pd.Timestamp | str, end: pd.Timestamp | str) -> pd.DataFrame:
    """
    Reindex one symbol's 1-min bars onto the full grid of regular-session minutes.

    Missing minutes become flat zero-volume bars at the last close, matching
    the streaming path's carry forward. Bars outside regular sessions are dropped.

    Args:
        pdf (pd.DataFrame): Input dataframe containing stock data.
        start (pd.Timestamp): UTC timestamp for the start of the window.
        end (pd.Timestamp): UTC timestamp for the end of the window. 
    """

    sessions = get_sessions(start, end) # market_open / market_close, tz-aware UTC
    session_grids = {
        session: pd.date_range(open_, close, freq="1min", inclusive="left").tz_localize(None)
        for session, open_, close in zip(sessions.index, sessions["market_open"], sessions["market_close"])
    } # Twelve Data (timezone="UTC") returns naive UTC
    grid = pd.DatetimeIndex([ts for minutes in session_grids.values() for ts in minutes])
    session_of = pd.Series(
        [session for session, minutes in session_grids.items() for _ in minutes], index=grid
    )

    symbol = pdf["symbol"].iloc[0]
    pdf = pdf.set_index("event_timestamp").sort_index().reindex(grid)

    # Drop sessions that are mostly vendor gaps, or they'd be flat-filled into fake bars
    coverage = pdf["close"].notna().groupby(session_of).mean()
    sparse = coverage[coverage < MIN_SESSION_COVERAGE]
    for session, frac in sparse.items():
        logger.warning("%s %s: %.0f%% of minutes have bars, dropping session", symbol, session.date(), frac * 100)
    pdf = pdf[~session_of.isin(sparse.index)]

    pdf["close"] = pdf["close"].ffill()
    for col in ("open", "high", "low"):
        pdf[col] = pdf[col].fillna(pdf["close"])
    pdf["volume"] = pdf["volume"].fillna(0)
    pdf["volume"] = pdf["volume"].astype("int64")
    pdf["symbol"] = symbol

    # Leading minutes before the first real trade have nothing to carry forward
    pdf = pdf.dropna(subset=["close"])
    return pdf.rename_axis("event_timestamp").reset_index()

def main():
    raw_dir = settings.data_dir / "raw"
    frames = []

    for ticker in settings.tickers:
        pdf = pd.read_parquet(raw_dir / f"symbol={ticker}")
        pdf.insert(0, "symbol", ticker)
        pdf = pdf.drop_duplicates(subset="event_timestamp", keep="last").sort_values("event_timestamp")
        start, end = pdf["event_timestamp"].min(), pdf["event_timestamp"].max()
        frames.append(reindex_to_minute_grid(pdf, start, end))

    pandas_df = pd.concat(frames, ignore_index=True)

    sessions_pdf = get_sessions(pandas_df["event_timestamp"].min(), pandas_df["event_timestamp"].max())
    sessions_pdf = sessions_pdf.rename_axis("session_date").reset_index()
    sessions_pdf["session_date"] = sessions_pdf["session_date"].dt.date

    for col in ("market_open", "market_close"):
        sessions_pdf[col] = sessions_pdf[col].dt.tz_convert(None)   # naive UTC, like event_timestamp

    spark = (
        SparkSession.builder.appName("historical_backfill")
        .config(map={
            "spark.sql.session.timeZone": "UTC",
            "spark.jars.packages": "org.postgresql:postgresql:42.7.4",
            "spark.sql.execution.arrow.pyspark.enabled": "true",
            "spark.driver.memory": "8g",
            "spark.driver.extraJavaOptions": "-Duser.timezone=UTC",
            "spark.executor.extraJavaOptions": "-Duser.timezone=UTC"
        })
        .getOrCreate()
    )
    df = spark.createDataFrame(pandas_df)
    sessions = spark.createDataFrame(sessions_pdf[["session_date", "market_open", "market_close"]])

    df = compute_features(df, sessions)
    write_to_postgres(df)

if __name__ == "__main__":
    main()