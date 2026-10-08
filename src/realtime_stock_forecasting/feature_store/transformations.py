from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

MARKET_TZ = "America/New_York"
NEAR_WINDOW_MIN = 15

LAGS = (1, 5, 15)
ROLL_WINDOWS = (5, 15, 60)

def _ordered() -> Window:
    return Window.partitionBy("symbol").orderBy("event_timestamp")

def _w(preceding: int) -> Window:
    """Trailing window of preceding prior bars plus the current one, per symbol"""
    return (Window.partitionBy("symbol").orderBy("event_timestamp").rowsBetween(-preceding, 0))

def _full_window_only(col, n_rows: int, window_rows: int):
    """Null out values until a full window of history exists"""
    return F.when(n_rows >= window_rows, col)

def add_lag_features(df: DataFrame) -> DataFrame:
    """
    Add lag features to the DataFrame.
    
    Args:
        df (DataFrame): Input DataFrame containing stock data.
    """

    for lag in LAGS:
        df = df.withColumn(f"lag_{lag}_close", F.lag("close", lag).over(_ordered()))

    return df.withColumn("lag_1_return", (F.col("close") / F.col("lag_1_close") - 1) * 100)

def add_rolling_features(df: DataFrame) -> DataFrame:
    """
    Add rolling features to the DataFrame.
    
    Args:
        df (DataFrame): Input DataFrame containing stock data.
    """

    bar_index = F.row_number().over(_ordered())

    for window in ROLL_WINDOWS:
        w = _w(window - 1)
        df = df.withColumn(f"close_rolling_mean_{window}min", _full_window_only(F.avg("close").over(w), bar_index, window))
        df = df.withColumn(f"return_rolling_std_{window}min", _full_window_only(F.stddev("lag_1_return").over(w), bar_index, window + 1))

    for window in (15, 60):
        w = _w(window - 1)
        df = df.withColumn(f"low_{window}min", _full_window_only(F.min("close").over(w), bar_index, window))
        df = df.withColumn(f"high_{window}min", _full_window_only(F.max("close").over(w), bar_index, window))

    return df.withColumn("volume_mean_15min", _full_window_only(F.avg("volume").over(_w(14)), bar_index, 15))

def add_time_features(df: DataFrame, sessions: DataFrame) -> DataFrame:
    """
    Add time-based features to the DataFrame.
    
    Args:
        df (DataFrame): Input DataFrame containing stock data.
        sessions (DataFrame): session_date, market_open, and market_close as UTC timestamps.
    """

    df = df.withColumn("session_date", F.to_date(F.from_utc_timestamp("event_timestamp", MARKET_TZ)))
    df = df.join(F.broadcast(sessions), on="session_date", how="left")

    since_open = (F.unix_timestamp("event_timestamp") - F.unix_timestamp("market_open")) / 60
    until_close = (F.unix_timestamp("market_close") - F.unix_timestamp("event_timestamp")) / 60

    df = df.withColumn("minutes_since_market_open", since_open.cast("int"))
    df = df.withColumn("day_of_week", F.weekday(F.from_utc_timestamp("event_timestamp", MARKET_TZ)))
    df = df.withColumn("is_near_open", (since_open >= 0) & (since_open < NEAR_WINDOW_MIN))
    df = df.withColumn("is_near_close", (until_close > 0) & (until_close <= NEAR_WINDOW_MIN))

    return df.drop("session_date", "market_open", "market_close")

def add_target(df: DataFrame) -> DataFrame:
    """
    Add target variable to the DataFrame.
    
    Args:
        df (DataFrame): Input DataFrame containing stock data.
    """

    return df.withColumn("next_close", F.lead("close", 1).over(_ordered()))


def compute_features(df: DataFrame, sessions: DataFrame, include_target: bool = True) -> DataFrame:
    """
    Compute features for the given DataFrame.

    Args:
        df (DataFrame): Input DataFrame containing stock data.
        sessions (DataFrame): DataFrame containing session information.
        include_target (bool): Whether to include the target variable in the output.
    """

    df = add_lag_features(df)
    df = add_rolling_features(df)
    df = add_time_features(df, sessions)
    if include_target:
        df = add_target(df)
    return df
