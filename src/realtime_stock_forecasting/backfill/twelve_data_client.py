import pandas as pd
from twelvedata import TDClient

MAX_OUTPUTSIZE = 5000


class TwelveDataClient:
    def __init__(self, api_key: str):
        self._client = TDClient(apikey=api_key)

    def get_time_series(
        self,
        symbol: str,
        interval: str,
        start: str,
        end: str,
        timezone: str = "UTC",
        outputsize: int = MAX_OUTPUTSIZE,
        adjust: str = "splits"
    ) -> pd.DataFrame:
        """
        Fetches time series data for a given symbol and interval from the Twelve Data API.

        The SDK defaults outputsize to 30 even when dates are given, so it is passed explicitly.
        The returned frame has `datetime` as the index, sorted newest first.

        Args:
            symbol (str): The stock symbol to fetch data for.
            interval (str): The interval for the time series data (e.g., '1min', '5min', '1h', '1d').
            start (str): Start of the window, 'YYYY-MM-DD HH:MM:SS' in `timezone`.
            end (str): End of the window, 'YYYY-MM-DD HH:MM:SS' in `timezone`.
            timezone (str): The timezone for the time series data (default is "UTC").
            outputsize (int): Max rows to return (API cap is 5,000).
        """
        ts_data = self._client.time_series(
            symbol=symbol,
            interval=interval,
            start_date=start,
            end_date=end,
            timezone=timezone,
            outputsize=outputsize,
            adjust=adjust
        ).as_pandas()
        return ts_data

    def get_earliest_timestamp(self, symbol: str, interval: str) -> pd.Timestamp:
        """
        Fetches the first available bar time for a symbol at a given interval (1 API credit).

        Args:
            symbol (str): The stock symbol to look up.
            interval (str): The interval to check (e.g., '1min').

        Returns:
            pd.Timestamp: Naive UTC timestamp of the earliest available bar.
        """
        resp = self._client.get_earliest_timestamp(symbol=symbol, interval=interval).as_json()
        return pd.Timestamp(resp["unix_time"], unit="s")
