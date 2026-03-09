"""Central configuration module.

All settings come from environment variables (with .env fallback).
YAML config files (selectors, defaults) are loaded once and cached.
"""
from __future__ import annotations

import functools
from pathlib import Path

import yaml
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Database
    database_url: str = Field(
        default=f"sqlite+aiosqlite:///{BASE_DIR}/data/scraper.db"
    )

    # Browser
    headless: bool = Field(default=True)
    browser_timeout: int = Field(default=30_000)  # ms
    user_agent: str = Field(
        default=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/125.0.0.0 Safari/537.36"
        )
    )

    # Rate limiting
    min_delay: float = Field(default=2.5)   # seconds
    max_delay: float = Field(default=6.0)
    max_concurrency: int = Field(default=1)

    # Scraping limits
    max_pages: int = Field(default=20)
    max_listings: int = Field(default=500)

    # Storage
    save_raw: bool = Field(default=True)
    raw_dir: Path = Field(default=BASE_DIR / "data" / "raw")
    output_dir: Path = Field(default=BASE_DIR / "data" / "exports")
    log_dir: Path = Field(default=BASE_DIR / "data" / "logs")

    # Retry
    max_retries: int = Field(default=3)
    retry_backoff: float = Field(default=2.0)

    # Logging
    log_level: str = Field(default="INFO")


@functools.lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


@functools.lru_cache(maxsize=1)
def load_selectors() -> dict:
    path = BASE_DIR / "configs" / "selectors.yaml"
    with open(path) as fh:
        return yaml.safe_load(fh)


@functools.lru_cache(maxsize=1)
def load_defaults() -> dict:
    path = BASE_DIR / "configs" / "defaults.yaml"
    with open(path) as fh:
        return yaml.safe_load(fh)
