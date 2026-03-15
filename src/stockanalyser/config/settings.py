"""
settings.py
-----------
Single source of truth for all runtime configuration.
"""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv


def _load_env() -> None:
    explicit = os.environ.get("DOTENV_PATH")
    if explicit and Path(explicit).exists():
        load_dotenv(dotenv_path=explicit, override=False)
        return

    cwd_candidate = Path.cwd() / "local.env"
    if cwd_candidate.exists():
        load_dotenv(dotenv_path=cwd_candidate, override=False)
        return

    for parent in Path(__file__).resolve().parents:
        candidate = parent / "local.env"
        if candidate.exists():
            load_dotenv(dotenv_path=candidate, override=False)
            return


_load_env()


from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file="local.env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ------------------------------------------------------------------ #
    #  API                                                                 #
    # ------------------------------------------------------------------ #
    api_key: str = Field(..., description="Polygon.io API key (required)")
    api_base_url: str = Field(default="https://api.polygon.io")

    # ------------------------------------------------------------------ #
    #  Date range                                                          #
    # ------------------------------------------------------------------ #
    start_date: str = Field(default="2025-01-01")
    end_date: str = Field(default="2025-12-31")

    # ------------------------------------------------------------------ #
    #  Spark                                                               #
    # ------------------------------------------------------------------ #
    spark_app_name: str = Field(default="StockAnalyser")
    spark_master: str = Field(default="local[*]")

    # ------------------------------------------------------------------ #
    #  Paths                                                               #
    # ------------------------------------------------------------------ #
    output_dir: Path = Field(default=Path("output"))
    input_file: Path = Field(default=Path("data/stocks_list.csv"))

    # ------------------------------------------------------------------ #
    #  Investment parameters                                               #
    # ------------------------------------------------------------------ #
    total_investment: float = Field(default=1_000_000.0)
    investment_per_stock: float = Field(default=10_000.0)

    # ------------------------------------------------------------------ #
    #  API request settings                                                #
    # ------------------------------------------------------------------ #
    api_request_timeout: int = Field(default=30)
    api_max_retries: int = Field(default=3)
    api_retry_backoff: float = Field(default=1.5)
    api_request_delay: float = Field(
        default=12.0,
        description="Seconds to wait between requests. 12s = 5 req/min (free tier). Set to 0 for paid tiers.",
    )
    api_max_workers: int = Field(
        default=5,
        description=(
            "Concurrent threads for API fetching. "
            "Free tier: 2, Starter: 5, Developer+: 10-20"
        ),
    )

    # ------------------------------------------------------------------ #
    #  Computed properties                                                 #
    # ------------------------------------------------------------------ #
    @computed_field
    @property
    def api_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.api_key}"}


settings = Settings()