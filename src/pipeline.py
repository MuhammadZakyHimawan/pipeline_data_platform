from __future__ import annotations

import re
import time

from .shared.logging_utils import setup_logging
from .shared.periods import resolve_period
from .shared.config import get_source_config

log = setup_logging()

# Registry pipe: source -> modul extract/transform/load
# Modul ditambahkan bertahap; semua sumber diharapkan mengikuti kontrak:
#   extract.extract(period=None, dry_run=False) -> dict[dataset, DataFrame]
#   transform.transform(period=None, dry_run=False) -> dict[dataset, DataFrame]
#   load.load(period=None, dry_run=False) -> dict[dataset, DataFrame]
_PIPES: dict[str, dict] = {
    "bps": {
        "extract": "src.extract.bps_scraper",
        "transform": "src.transform.bps_transform",
        "load": "src.load.bps_load",
    },
    "kurs": {
        "extract": "src.extract.kurs_scraper",
        "transform": "src.transform.kurs_transform",
        "load": "src.load.kurs_load",
    },
    "hari_libur": {
        "extract": "src.extract.hari_libur_scraper",
        "transform": "src.transform.hari_libur_transform",
        "load": "src.load.hari_libur_load",
    },
    "gdelt": {
        "extract": "src.extract.gdelt_scraper",
        "transform": "src.transform.gdelt_transform",
        "load": "src.load.gdelt_load",
    },
    "cuaca": {
        "extract": "src.extract.cuaca_scraper",
        "transform": "src.transform.cuaca_transform",
        "load": "src.load.cuaca_load",
    },
    "poi": {
        "extract": "src.extract.poi_scraper",
        "transform": "src.transform.poi_transform",
        "load": "src.load.poi_load",
    },
    "event": {
        "extract": "src.extract.event_scraper",
        "transform": "src.transform.event_transform",
        "load": "src.load.event_load",
    },
    "trends": {
        "extract": "src.extract.trends_scraper",
        "transform": "src.transform.trends_transform",
        "load": "src.load.trends_load",
    },
    "gmaps": {
        "extract": "src.extract.gmaps_harga_scraper",
        "transform": "src.transform.gmaps_harga_transform",
        "load": "src.load.gmaps_harga_load",
    },
    "traveloka_review": {
        "extract": "src.extract.traveloka_review_scraper",
    },
    "transport": {
        "extract": "src.extract.transport_scraper",
        "transform": "src.transform.transport_transform",
        "load": "src.load.transport_load",
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

    'src.extract.bps_scraper' -> 'src/extract/bps_scraper.py'
    """
    fqnm = _PIPES.get(source, {}).get(stage, "")
    return fqnm.replace(".", "/") + ".py" if fqnm else f"{stage}/<belum?>.py"


def run_stage(source: str, stage: str, period: str | None = None, dry_run: bool = False) -> dict:
    """Jalankan satu tahap (extract/transform/load) untuk satu sumber."""
    if source not in _PIPES:
        raise ValueError(
            f"source '{source}' belum terdaftar. Tersedia: {get_registered_sources()} "
            f"| belum diimplementasi: {sorted(PENDING_SOURCES)}"
        )
    if stage not in ("extract", "transform", "load"):
        raise ValueError("stage harus salah satu dari: extract, transform, load")

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
    result = module.extract(period=period, dry_run=dry_run) if stage == "extract" else (
        module.transform(period=period, dry_run=dry_run) if stage == "transform" else module.load(period=period, dry_run=dry_run)
    )
    elapsed = time.time() - start
    log.info("=== %s.%s selesai (%.1f dtk, %d dataset) ===", source, stage, elapsed, len(result))
    return result


def run_pipeline(source: str, period: str | None = None, dry_run: bool = False) -> dict[str, dict]:
    """Jalankan ETL penuh (extract -> transform -> load) untuk satu sumber."""
    summary: dict[str, dict] = {}
    stages = ("extract", "transform", "load")
    for stage in stages:
        result = run_stage(source, stage, period=period, dry_run=dry_run)
        summary[stage] = {k: len(v) for k, v in result.items()}
    return summary