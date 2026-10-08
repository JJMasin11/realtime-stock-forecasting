from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT_DIR = Path(__file__).resolve().parents[3]

class Settings(BaseSettings):
    finnhub_api_key: str
    twelve_data_api_key: str
    kafka_bootstrap_servers: str
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str
    postgres_user: str
    postgres_password: str
    redis_url: str
    mlflow_tracking_uri: str
    tickers: list[str] = ["AAPL", "TSLA", "JPM", "JNJ", "KO"]
    kafka_topic: str = "stock-trades"
    data_dir: Path = ROOT_DIR / "data"

    @property
    def postgres_jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    model_config = SettingsConfigDict(env_file=ROOT_DIR / "infra" / ".env")

settings = Settings()