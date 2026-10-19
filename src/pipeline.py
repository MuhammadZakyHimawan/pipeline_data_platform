from __future__ import annotations

import inspect
import re
import time

from .shared.logging_utils import setup_logging
from .shared.periods import resolve_period
from .shared.config import get_source_config

log = setup_logging()

# Registry pipe: source -> modul bronze (extract only)
# Modul ditambahkan bertahap; semua sumber diharapkan mengikuti kontrak:
#   bronze.extract(period=None, dry_run=False) -> dict[dataset, DataFrame]
_PIPES: dict[str, dict] = {
    "bps": {
        "bronze": "src.bronze.bps_scraper",
    },
    "kurs": {
        "bronze": "src.bronze.kurs_scraper",
    },
    "hari_libur": {
        "bronze": "src.bronze.hari_libur_scraper",
    },
    "gdelt": {
        "bronze": "src.bronze.gdelt_scraper",
    },
    "cuaca": {
        "bronze": "src.bronze.cuaca_scraper",
    },
    "poi": {
        "bronze": "src.bronze.poi_scraper",
    },
    "event": {
        "bronze": "src.bronze.event_scraper",
    },
    "trends": {
        "bronze": "src.bronze.trends_scraper",
    },
    "gmaps": {
        "bronze": "src.bronze.gmaps_harga_scraper",
    },
    "traveloka_review": {
        "bronze": "src.bronze.traveloka_review_scraper",
    },
    "transport": {
        "bronze": "src.bronze.transport_scraper",
    },
}

# Sumber yang terdaftar di config/sources.yaml tetapi belum diimplementasi
PENDING_SOURCES: set[str] = set()

_import_cache: dict[str, object] = {}


def _import_module(fqnm: str) -> object:
    if fqnm not in _import_cache:
        import importlib

        _import_cache[fqnm] = importlib.import_module(fqnm)
    return _import_cache[fqnm]


def get_registered_sources() -> list[str]:
    return sorted(_PIPES.keys())


def get_stage_path(source: str, stage: str) -> str:
    """Path file .py dari registry pipe untuk satu sumber & tahap.

    'src.bronze.bps_scraper' -> 'src/bronze/bps_scraper.py'
    """
    fqnm = _PIPES.get(source, {}).get(stage, "")
    return fqnm.replace(".", "/") + ".py" if fqnm else f"{stage}/<belum?>.py"


def run_stage(
    source: str, 
    stage: str, 
    period: str | None = None, 
    dry_run: bool = False,
    force_login: bool = False,
    headless_login: bool = False,
    keep_browser_open: bool = False
) -> dict:
    """Jalankan tahap bronze (extract) untuk satu sumber."""
    if source not in _PIPES:
        raise ValueError(
            f"source '{source}' belum terdaftar. Tersedia: {get_registered_sources()} "
            f"| belum diimplementasi: {sorted(PENDING_SOURCES)}"
        )
    if stage != "bronze":
        raise ValueError("stage saat ini hanya mendukung: bronze")

    # validasi periode sejak awal (gdelt: izinkan YYYYMMDD untuk backfill tanggal)
    if source == "gdelt" and period and re.fullmatch(r"\d{8}", period.strip()):
        period_label = f"{period[:4]}-{period[4:6]}-{period[6:]}"
    else:
        period_label, _ = resolve_period(period)
    src_cfg = get_source_config(source)
    enabled = src_cfg.get("enabled", True)
    if not enabled:
        log.warning("source '%s' disabled di config/sources.yaml", source)

    module = _import_module(_PIPES[source][stage])
    start = time.time()
    log.info("=== %s.%s mulai (periode %s) ===", source, stage, period_label)
    sig = inspect.signature(module.extract)
    kwargs = {"period": period, "dry_run": dry_run}
    if "force_login" in sig.parameters:
        kwargs["force_login"] = force_login
    if "headless_login" in sig.parameters:
        kwargs["headless_login"] = headless_login
    if "keep_browser_open" in sig.parameters:
        kwargs["keep_browser_open"] = keep_browser_open
    result = module.extract(**kwargs)
    elapsed = time.time() - start
    log.info("=== %s.%s selesai (%.1f dtk, %d dataset) ===", source, stage, elapsed, len(result))
    return result


def run_pipeline(
    source: str, 
    period: str | None = None, 
    dry_run: bool = False,
    force_login: bool = False,
    headless_login: bool = False
) -> dict[str, dict]:
    """Jalankan pipeline bronze (extract) untuk satu sumber."""
    summary: dict[str, dict] = {}
    result = run_stage(source, "bronze", period=period, dry_run=dry_run, force_login=force_login, headless_login=headless_login)
    summary["bronze"] = {k: len(v) for k, v in result.items()}
    return summary