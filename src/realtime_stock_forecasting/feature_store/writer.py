from pyspark.sql import DataFrame

from realtime_stock_forecasting.utils.config import settings


def write_to_postgres(df: DataFrame):
    """
    Write the DataFrame to a PostgreSQL database.
    
    Args:
        df (DataFrame): Input DataFrame containing stock data.
    """
    df.write.jdbc(
        url=settings.postgres_jdbc_url,
        table="historical_stock_data",
        mode="overwrite",
        properties={
            "user": settings.postgres_user,
            "password": settings.postgres_password,
            "driver": "org.postgresql.Driver",
            "truncate": "true",
            "batchsize": "10000"
        }
    )