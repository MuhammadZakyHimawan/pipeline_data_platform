from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_LAKE_DIR = PROJECT_ROOT / "data_lake"


@lru_cache(maxsize=1)
def load_env() -> None:
    env_path = CONFIG_DIR / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=False)


@lru_cache(maxsize=1)
def get_settings() -> dict:
    load_env()
    return {
        "db": {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": int(os.getenv("DB_PORT", "5432")),
            "name": os.getenv("DB_NAME", "hotel_data"),
            "user": os.getenv("DB_USER", "postgres"),
            "pass": os.getenv("DB_PASS", "change_me"),
            "schema_gold": os.getenv("DB_SCHEMA_GOLD", "gold"),
        },
        "bps_api_key": os.getenv("BPS_API_KEY", ""),
        "llm": {
            "url": os.getenv("LLM_API_URL", ""),
            "model": os.getenv("LLM_MODEL", ""),
            "key": os.getenv("LLM_API_KEY", ""),
        },
    }


def get_db_config() -> dict:
    return get_settings()["db"]


def get_bps_api_key() -> str:
    return get_settings()["bps_api_key"]


@lru_cache(maxsize=1)
def load_sources() -> dict:
    path = CONFIG_DIR / "sources.yaml"
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_source_config(source: str) -> dict:
    cfg = load_sources()
    defaults = cfg.get("defaults", {})
    src = cfg.get("sources", {}).get(source)
    if src is None:
        raise KeyError(f"source '{source}' tidak terdaftar di config/sources.yaml")
    merged = dict(defaults)
    merged.update(src)
    return merged


# Grain penamaan file di data_lake (lihat config/sources.yaml):
#   year     -> satu file per tahun: {dataset}/{YYYY}.parquet
#   snapshot -> satu file penuh tanpa waktu: {dataset}/data.parquet
GRAIN_YEAR = "year"
GRAIN_SNAPSHOT = "snapshot"


def get_grain(source: str) -> str:
    """Grain sumber: 'year' (file per tahun) atau 'snapshot' (satu data.parquet)."""
    grain = (get_source_config(source).get("grain") or GRAIN_YEAR).strip().lower()
    if grain not in (GRAIN_YEAR, GRAIN_SNAPSHOT):
        raise ValueError(
            f"grain '{grain}' untuk source '{source}' tidak dikenal. "
            f"Gunakan {GRAIN_YEAR} atau {GRAIN_SNAPSHOT}."
        )
    return grain


def get_start_year(source: str, default: int = 2014) -> int:
    """Tahun pertama histori yang ditarik untuk sumber (dari config, default 2014)."""
    try:
        return int(get_source_config(source).get("start_year", default))
    except (TypeError, ValueError):
        return default


def get_sources_by_schedule(schedule: str) -> list[str]:
    """Kembalikan list nama sumber yang memiliki schedule tertentu dan enabled: true."""
    cfg = load_sources()
    return [
        name for name, src in cfg.get("sources", {}).items()
        if src.get("schedule") == schedule and src.get("enabled", True)
    ]
