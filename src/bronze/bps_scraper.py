from __future__ import annotations

import json
import logging
import time
from datetime import datetime

import pandas as pd
import requests

from ..shared.config import get_bps_api_key, get_source_config
from ..shared.periods import resolve_years, resolve_daily_period, resolve_historical_period
from ..shared.row_limit import truncate_df
from ..shared.storage import write_parquet_by_year
from ..shared.run_options import get_historis

log = logging.getLogger(__name__)

SOURCE = "bps"

BASE_URL = (
    "https://webapi.bps.go.id/v1/api/list/model/data/lang/ind/"
    "domain/{domain}/var/{var}/th/{th}/key/{key}"
)

# Nama dataset -> kelompok parameter (domain, var, start_year)
DATASET_KEYS = [
    "asal_turis",
    "inflasi",
    "lama_menginap",
    "tpk_bintang",
    "wisman_pintu_masuk",
    "wisnus_tujuan",
]


def _fetch_payload(api_key: str, domain: str, var: str, year: int, retries: int = 4) -> dict | None:
    """Ambil satu tahun dari BPS Web API. th = tahun - 1900."""
    th = year - 1900
    url = BASE_URL.format(domain=domain, var=var, th=th, key=api_key)
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "OK":
                    return data
                log.warning("BPS status!=OK utk %s th=%s: %s", var, th, data.get("status"))
                return None
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(3 * attempt)
                continue
            log.warning("BPS HTTP %s utk %s th=%s", resp.status_code, var, th)
            return None
        except requests.RequestException as exc:
            log.warning("BPS request error utk %s th=%s (attempt %s): %s", var, th, attempt, exc)
            time.sleep(3 * attempt)
    return None


def _build_meta_json(payload: dict) -> str:
    """Simpan metadata payload (var/turvar/vervar/turtahun/tahun) sebagai JSON string untuk transform."""
    meta = {
        "var": payload.get("var", []),
        "turvar": payload.get("turvar", []),
        "vervar": payload.get("vervar", []),
        "turtahun": payload.get("turtahun", []),
        "tahun": payload.get("tahun", []),
    }
    return json.dumps(meta, ensure_ascii=False)


def extract_dataset(
    dataset: str,
    api_key: str,
    years: list[int],
    period_label: str,
    dataset_cfg: dict,
    dry_run: bool = False,
) -> dict[int, pd.DataFrame]:
    """EXTRACT (bronze): ambil payload BPS mentah untuk daftar tahun.

    Bronze ditulis **satu file per tahun**: {dataset}/{YYYY}.parquet, tiap file
    berisi satu baris (tahun + datacontent/meta mentah sebagai JSON string).
    File langsung ditulis begitu payload diterima, jadi run bisa dihentikan dan
    dilanjutkan tanpa mengulang tahun yang sudah ada.

    Returns:
        dict[tahun, DataFrame] - hanya tahun yang benar-benar dapat datanya.
    """
    domain = dataset_cfg["domain"]
    var = str(dataset_cfg["var"])
    boleh_tulis = not dry_run

    rows: dict[int, pd.DataFrame] = {}
    gagal: list[int] = []
    for year in years:
        payload = _fetch_payload(api_key, domain, var, year)
        if payload is None:
            log.warning("bps/%s: payload kosong utk tahun %s", dataset, year)
            gagal.append(year)
            continue
        datacontent = payload.get("datacontent")
        if not datacontent:
            log.info("bps/%s: datacontent kosong utk tahun %s (dilewati)", dataset, year)
            gagal.append(year)
            continue

        baris = pd.DataFrame(
            [
                {
                    "tahun": year,
                    "domain": domain,
                    "var": var,
                    "datacontent_json": json.dumps(datacontent, ensure_ascii=False),
                    "payload_meta_json": _build_meta_json(payload),
                }
            ]
        )
        baris = truncate_df(baris)
        rows[year] = baris

        if boleh_tulis:
            write_parquet_by_year(baris, "bronze", SOURCE, dataset, year_col="tahun")
        else:
            log.info("[dry-run] bps/%s bronze: %d tahun (tidak ditulis)", dataset, len(rows))
        time.sleep(1.5)  # jeda antar permintaan API

    if not rows:
        log.warning(
            "bps/%s: tidak ada payload yang berhasil diambil (range %s..%s)",
            dataset,
            min(years),
            max(years),
        )
        return {}

    log.info(
        "bps/%s bronze: %d/%d tahun ditulis (%s)",
        dataset,
        len(rows),
        len(years),
        ", ".join(str(y) for y in sorted(rows)),
    )
    return rows


def extract(period: str | None = None, dry_run: bool = False, historis: bool = False) -> dict[str, pd.DataFrame]:
    """EXTRACT semua dataset BPS -> data_lake/bronze/bps/{dataset}/{YYYY}.parquet

    Default ('latest'): tarik riwayat penuh tiap dataset dari start_year s.d. tahun
    berjalan (lihat config/sources.yaml), satu file per tahun.

    Periode eksplisit ('YYYY' atau 'YYYY-YYYY') membatasi tahun yang ditarik;
    file per tahun yang sudah ada tidak dihapus/diubah di luar rentang itu.
    
    Mode Harian (default): hanya tahun berjalan (current year)
    Mode Historis (--historis): full backfill dari start_year s.d. tahun berjalan"""
    api_key = get_bps_api_key()
    if not api_key:
        raise RuntimeError(
            "BPS_API_KEY kosong. Isi di config/.env (lihat config/.env.example)."
        )

    src_cfg = get_source_config(SOURCE)
    now_year = datetime.now().year
    log.info("bps extract: tahun berjalan=%s", now_year)

    # Determine period based on mode
    if historis or get_historis():
        period = resolve_historical_period(SOURCE)
        log.info("bps: MODE HISTORIS aktif, periode=%s", period)
    else:
        period = resolve_daily_period(SOURCE)
        log.info("bps: MODE HARIAN aktif, periode=%s", period)

    # Periode eksplisit? (bukan None / latest / kosong)
    p = (period or "").strip()
    eksplisit = bool(p) and p.lower() not in ("latest", "terbaru")

    datasets_cfg: dict = src_cfg.get("datasets", {})
    hasil: dict[str, pd.DataFrame] = {}
    gabung: dict[str, pd.DataFrame] = {}

    for dataset in DATASET_KEYS:
        dataset_cfg = datasets_cfg.get(dataset)
        if not dataset_cfg:
            log.warning("bps/%s: tidak ada konfigurasi, dilewati", dataset)
            continue

        start = int(dataset_cfg.get("start_year", 2014))
        if eksplisit:
            tahun_list = resolve_years(p)
            tahun_list = [y for y in tahun_list if y >= start]
        else:
            tahun_list = list(range(start, now_year + 1))

        if not tahun_list:
            log.info("bps/%s: tidak ada tahun dalam rentang (start=%s)", dataset, start)
            continue
        log.info("bps/%s: riwayat %s..%s", dataset, tahun_list[0], tahun_list[-1])

        per_tahun = extract_dataset(
            dataset=dataset,
            api_key=api_key,
            years=tahun_list,
            period_label=str(now_year),
            dataset_cfg=dataset_cfg,
            dry_run=dry_run,
        )
        if not per_tahun:
            continue
        hasil[dataset] = pd.concat(list(per_tahun.values()), ignore_index=True)
        gabung[dataset] = hasil[dataset]

    return hasil