from __future__ import annotations

import json
import logging
import time
from pathlib import Path

import pandas as pd
import requests

from ..shared.config import get_source_config
from ..shared.storage import read_parquet, write_parquet

log = logging.getLogger(__name__)

SOURCE = "transport"
DATASET = "rute"
LABEL_DEFAULT = "latest"

CACHE_GEO = Path("data_lake") / "cache" / "transport_geo_cache.json"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {"User-Agent": "JXB_DSS_Pipeline/1.0"}
BUDGET_GEOCODE_DETIK = 300
JEDA_NOMINATIM_DETIK = 0.5

KOLOM_SILVER = {
    "jenis": ["nama_jenis"],
    "rute": ["nama_rute", "nama_jenis", "kode_rute"],
    "titik": ["nama_titik", "latitude", "longitude", "alamat", "wilayah"],
    "pemetaan": ["nama_rute", "nama_titik", "urutan_berhenti"],
}


def _label(cfg: dict, period: str | None) -> str:
    return (period or cfg.get("period", LABEL_DEFAULT)).strip().lower()


def _muat_cache_geo() -> dict:
    if CACHE_GEO.exists():
        try:
            return json.loads(CACHE_GEO.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _simpan_cache_geo(cache: dict) -> None:
    try:
        CACHE_GEO.parent.mkdir(parents=True, exist_ok=True)
        CACHE_GEO.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        log.warning("gagal simpan cache geocode transport: %s", e)


def _dapatkan_koordinat_osm(nama_halte: str, timeout: int = 30):
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": f"{nama_halte}, Jakarta",
        "format": "json",
        "limit": 1,
        "addressdetails": 1,
    }
    try:
        response = requests.get(url, params=params, headers=NOMINATIM_HEADERS, timeout=timeout)
        if response.status_code == 200:
            data = response.json()
            if data:
                hasil = data[0]
                addr = hasil.get("address") or {}
                kecamatan = (
                    addr.get("suburb")
                    or addr.get("county")
                    or addr.get("town")
                    or addr.get("municipality")
                )
                kota = addr.get("city_district") or addr.get("city")
                if kecamatan and kota and kecamatan != kota:
                    wilayah = f"{kecamatan}, {kota}"
                elif kecamatan or kota:
                    wilayah = kecamatan or kota
                else:
                    wilayah = None
                return (
                    float(hasil["lat"]),
                    float(hasil["lon"]),
                    hasil.get("display_name") or None,
                    wilayah,
                )
    except Exception as e:
        log.debug("nominatim transport gagal %s: %s", nama_halte, e)
    return None, None, None, None


def _geocode_missing(nama_unik: list[str], cache: dict, budget: int) -> dict:
    belum = [n for n in nama_unik if not cache.get(n, {}).get("alamat")]
    mulai = time.time()
    berhasil = gagal = dilewati = 0
    for nama in belum:
        if time.time() - mulai > budget:
            dilewati += 1
            continue
        lat, lon, alamat, wilayah = _dapatkan_koordinat_osm(nama)
        cache[nama] = {"lat": lat, "lon": lon, "alamat": alamat, "wilayah": wilayah}
        time.sleep(JEDA_NOMINATIM_DETIK)
        if alamat:
            berhasil += 1
        else:
            gagal += 1
    if belum:
        log.info(
            "transport geocode: nominatim berhasil=%d gagal=%d dilewati(budget)=%d",
            berhasil,
            gagal,
            dilewati,
        )
        _simpan_cache_geo(cache)
    return cache


def _bersihkan(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for kol in ("nama_jenis", "kode_rute", "nama_rute"):
        out[kol] = out[kol].astype("string").str.strip().replace({"": "-"})
    out = out[out["nama_rute"] != "-"].reset_index(drop=True)
    # defensif: rute duplikat dari bronze digabung (ambil baris pertama)
    out = out.drop_duplicates(subset=["nama_rute"]).reset_index(drop=True)
    return out


def transform(period: str | None = None, dry_run: bool = False) -> dict[str, pd.DataFrame]:
    cfg = get_source_config(SOURCE)
    label = _label(cfg, period)
    budget = int(cfg.get("budget_geocode_detik", BUDGET_GEOCODE_DETIK))

    try:
        bronze = read_parquet("bronze", SOURCE, DATASET, label)
    except FileNotFoundError:
        log.warning("transport: bronze tidak ada utk %s", label)
        return {}

    out = _bersihkan(bronze)
    if out.empty:
        log.warning("transport: tidak ada rute valid")
        return {}

    # --- Normalisasi menjadi tabel relasional ---
    jenis_unik = sorted(out["nama_jenis"].dropna().unique().tolist())
    df_jenis = pd.DataFrame({"nama_jenis": jenis_unik})

    df_rute = (
        out[["nama_rute", "nama_jenis", "kode_rute"]]
        .drop_duplicates(subset=["nama_rute"])
        .sort_values("nama_rute")
        .reset_index(drop=True)
    )
    df_rute = df_rute[["nama_rute", "nama_jenis", "kode_rute"]]

    # --- Titik / halte unik + geocode ---
    semua_titik: set[str] = set()
    pemetaan_list: list[dict] = []
    for _, r in out.iterrows():
        halte = json.loads(r.get("daftar_halte_json") or "[]")
        urutan = 1
        for nama_halte in halte:
            nama_halte = str(nama_halte).strip()
            if not nama_halte:
                continue
            semua_titik.add(nama_halte)
            pemetaan_list.append({
                "nama_rute": r["nama_rute"],
                "nama_titik": nama_halte,
                "urutan_berhenti": urutan,
            })
            urutan += 1

    cache = _muat_cache_geo()
    cache = _geocode_missing(sorted(semua_titik), cache, budget)

    df_titik = pd.DataFrame(
        [
            {
                "nama_titik": nama,
                "latitude": cache.get(nama, {}).get("lat"),
                "longitude": cache.get(nama, {}).get("lon"),
                "alamat": cache.get(nama, {}).get("alamat"),
                "wilayah": cache.get(nama, {}).get("wilayah"),
            }
            for nama in sorted(semua_titik)
        ],
        columns=KOLOM_SILVER["titik"],
    )
    df_pemetaan = pd.DataFrame(pemetaan_list, columns=KOLOM_SILVER["pemetaan"])

    result: dict[str, pd.DataFrame] = {}
    for dataset, cols in KOLOM_SILVER.items():
        df = {"jenis": df_jenis, "rute": df_rute, "titik": df_titik, "pemetaan": df_pemetaan}[dataset]
        df = df[cols].reset_index(drop=True)
        if df.empty:
            log.warning("transport silver %s kosong; dilewati", dataset)
        elif dry_run:
            log.info("[dry-run] transport silver %s: %d baris (tidak ditulis)", dataset, len(df))
        else:
            path = write_parquet(df, "silver", SOURCE, dataset, label)
            log.info("transport silver %s ditulis: %s (%d baris)", dataset, path, len(df))
        result[dataset] = df
    return result