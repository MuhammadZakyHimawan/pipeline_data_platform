from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import requests

from ..shared.config import get_source_config
from ..shared.periods import resolve_period
from ..shared.storage import read_parquet, write_parquet

log = logging.getLogger(__name__)

SOURCE = "event"
BRONZE_DATASET = "events"
DATASET = "agenda"
CACHE_GEO = Path("data_lake") / "cache" / "event_geo_cache.json"

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {"User-Agent": "JXB_DSS_Pipeline/1.0"}
BATAS_INDONESIA = {"lat": (-11.0, 6.0), "lon": (95.0, 141.0)}
BUDGET_GEOCODE_DETIK = 120

VENUE_ALIAS = {
    "jcc": "Jakarta Convention Center",
    "jakarta convention center": "Jakarta Convention Center",
    "jiexpo": "Jakarta International Expo",
    "jiec": "Jakarta International Expo",
    "ji expo": "Jakarta International Expo",
    "jakarta international expo": "Jakarta International Expo",
    "ice": "ICE BSD City",
    "ice bsd": "ICE BSD City",
    "ice bsd city": "ICE BSD City",
    "nice": "Nusantara International Convention Exhibition",
    "monas": "Monumen Nasional",
}

VENUE_KOORDINAT = {
    "jakarta international expo": (-6.1450, 106.8468),
    "jakarta convention center": (-6.2257, 106.8071),
    "ice bsd city": (-6.3021, 106.6446),
    "nusantara international convention exhibition": (-6.1080, 106.6800),
    "monumen nasional": (-6.1754, 106.8249),
    "balai kartini": (-6.2233, 106.8063),
    "senayan city": (-6.2297, 106.8031),
    "jakarta international stadium": (-6.1135, 106.8804),
    "kemchicks grand indonesia": (-6.1947, 106.8180),
    "the hermitage jakarta": (-6.1911, 106.8333),
    "mandiri hall": (-6.2257, 106.8071),
    "plenary hall jakarta convention center": (-6.2257, 106.8071),
    "istanbul convention centre": (-6.2257, 106.8071),
    "graha skb pusat": (-6.1811, 106.8333),
    "isma building": (-6.2250, 106.8070),
}

SUMBER_KOORDINAT = {
    "jiexpo": (-6.1450, 106.8468),
    "ice": (-6.3021, 106.6446),
    "nice": (-6.1080, 106.6800),
    "jcc": (-6.2257, 106.8071),
    "eventbrite": (-6.1754, 106.8249),
    "jakarta.go.id": (-6.1754, 106.8249),
}

_HALL_CODE_RE = re.compile(
    r"(?:^|\b)[a-z]{1,3}\d{1,3}(\s*[,&]\s*[a-z]{1,3}\d{1,3})*\b|"
    r"\bopen\s*space\b|\bwest\s*park\b|\bgrand\s*hall\b|\bcnv\b|"
    r"\bgambir\s*expo\b|\ball\s*(?:area|hall|meeting\s*room)\b|"
    r"\bpre[-\s]*function\b|\bbundaran\s*gambir\b|\barea\s*danau\b|"
    r"\bbelakang\s*kantor\b"
)
_ORG_RE = re.compile(
    r"^(?:asosiasi|badan|komite|dewan|kementerian|pt\b|cv\b|persero|"
    r"pengurus\s*pusat|ikatan|himpunan|pusat)\b"
)
_CODE_RE = re.compile(r"^[a-z\d\s,&-]{1,20}$")
_TRAILING_JUNK_RE = re.compile(r",\s*\d{1,2}\s+[a-z]+\s*[-–—]?\s*$|\s*[-–—]\s*$")

KOLOM_SILVER = [
    "id_event", "sumber", "judul", "tanggal_mulai", "tanggal_akhir",
    "lokasi_venue", "kategori", "url_detail", "url_gambar", "deskripsi",
    "tanggal_unggah", "latitude", "longitude",
]


def normalisasi_venue(nama) -> str | None:
    if nama is None or (isinstance(nama, float) and pd.isna(nama)) or (hasattr(nama, "notna") and not nama.notna()):
        return None
    teks = str(nama).strip().lower()
    teks = re.sub(r"\s+", " ", teks)
    teks = re.sub(r"[.,;:!?]+$", "", teks).strip()
    if not teks:
        return None
    return VENUE_ALIAS.get(teks, teks)


def bersihkan_nama_venue(kunci: str | None) -> tuple[str | None, bool]:
    if not kunci:
        return None, False
    kunci = kunci.strip().lower()
    kunci = _TRAILING_JUNK_RE.sub("", kunci).strip().rstrip(",").strip()
    if not kunci:
        return None, False
    for sep in (" · ", " • "):
        if sep in kunci:
            kunci = kunci.split(sep)[-1].strip()
    if _HALL_CODE_RE.search(kunci):
        return "jakarta international expo", False
    if _ORG_RE.search(kunci):
        return kunci, False
    if len(kunci) < 4 or _CODE_RE.match(kunci):
        return kunci, False
    return kunci, True


def _dapatkan_koordinat_nominatim(nama: str, timeout: int = 45, max_retries: int = 2):
    kandidat = [nama]
    if "jakarta" not in nama.lower():
        kandidat.append(f"{nama}, Jakarta")
    for q in kandidat:
        backoff = 2.0
        for percobaan in range(1, max_retries + 1):
            try:
                r = requests.get(
                    NOMINATIM_URL,
                    params={"q": q, "format": "json", "limit": 1, "addressdetails": 0},
                    headers=NOMINATIM_HEADERS,
                    timeout=timeout,
                )
                if r.status_code == 429:
                    time.sleep(int(r.headers.get("Retry-After", 10)))
                    continue
                r.raise_for_status()
                data = r.json()
                if data:
                    lat = float(data[0]["lat"])
                    lon = float(data[0]["lon"])
                    if (
                        BATAS_INDONESIA["lat"][0] <= lat <= BATAS_INDONESIA["lat"][1]
                        and BATAS_INDONESIA["lon"][0] <= lon <= BATAS_INDONESIA["lon"][1]
                    ):
                        return lat, lon
                    return None, None
                break
            except Exception as e:
                log.debug("nominatim gagal %s percobaan %s: %s", q, percobaan, e)
                if percobaan < max_retries:
                    time.sleep(backoff)
                    backoff *= 2
    return None, None


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
        log.warning("gagal simpan cache geocode: %s", e)


def _bersihkan(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["judul"] = out["judul"].astype("string").str.strip()
    out = out[out["judul"].notna() & (out["judul"] != "")].copy()

    for kol in ("tanggal_mulai", "tanggal_akhir", "tanggal_unggah"):
        out[kol] = pd.to_datetime(out[kol], errors="coerce")

    # tanggal_akhir < tanggal_mulai -> perbaiki (bandingkan sebagai datetime64, bukan object)
    bad = out["tanggal_akhir"].notna() & out["tanggal_mulai"].notna() & (out["tanggal_akhir"] < out["tanggal_mulai"])
    out.loc[bad, "tanggal_akhir"] = out.loc[bad, "tanggal_mulai"]

    for kol in ("tanggal_mulai", "tanggal_akhir", "tanggal_unggah"):
        out[kol] = out[kol].dt.date

    for kol in ("lokasi_venue", "kategori", "url_detail", "url_gambar", "deskripsi", "sumber"):
        out[kol] = out[kol].astype("string").str.strip().replace({"": None})

    return out.reset_index(drop=True)


def _id_event(sumber, judul, tanggal_mulai, url_detail) -> str:
    bahan = f"{sumber}|{judul}|{tanggal_mulai}|{url_detail}"
    return hashlib.md5(bahan.encode("utf-8")).hexdigest()


def _lengkapi_koordinat(out: pd.DataFrame) -> pd.DataFrame:
    cache = dict(VENUE_KOORDINAT)
    cache.update(_muat_cache_geo())

    venue_unik = sorted({kunci for kunci in (normalisasi_venue(v) for v in out["lokasi_venue"]) if kunci})
    perlu_nominatim: list[tuple[str, str]] = []
    for kunci in venue_unik:
        if kunci in cache:
            continue
        cleaned, geocodable = bersihkan_nama_venue(kunci)
        if not cleaned:
            continue
        if cleaned in cache:
            cache[kunci] = cache[cleaned]
            continue
        if geocodable:
            perlu_nominatim.append((cleaned, kunci))
        else:
            # tidak layak geocode; kalau bersih berbeda, pakai fallback sumber nanti
            pass

    log.info("event geocode: venue unik=%d, perlu Nominatim=%d", len(venue_unik), len(perlu_nominatim))

    mulai = time.time()
    berhasil = gagal = 0
    for i, (cleaned, kunci_asli) in enumerate(perlu_nominatim, 1):
        if time.time() - mulai > BUDGET_GEOCODE_DETIK:
            log.warning("geocode mencapai budget %ds; sisa di-skip", BUDGET_GEOCODE_DETIK)
            gagal += len(perlu_nominatim) - i + 1
            break
        lat, lon = _dapatkan_koordinat_nominatim(cleaned)
        time.sleep(1)
        if lat is not None and lon is not None:
            cache[kunci_asli] = (lat, lon)
            berhasil += 1
        else:
            cache[kunci_asli] = None
            gagal += 1
    if perlu_nominatim:
        log.info("event geocode: Nominatim berhasil=%d gagal=%d", berhasil, gagal)
        _simpan_cache_geo(cache)

    lat_list, lon_list = [], []
    for _, row in out.iterrows():
        kunci = normalisasi_venue(row["lokasi_venue"])
        koord = cache.get(kunci) if kunci else None
        if not koord:
            koord = SUMBER_KOORDINAT.get(row["sumber"])
        lat_list.append(koord[0] if koord else None)
        lon_list.append(koord[1] if koord else None)
    out["latitude"] = lat_list
    out["longitude"] = lon_list
    return out


def transform(period: str | None = None, dry_run: bool = False) -> dict[str, pd.DataFrame]:
    _, _ = resolve_period(period)
    label = (period or "latest").strip().lower()

    try:
        bronze = read_parquet("bronze", SOURCE, BRONZE_DATASET, label)
    except FileNotFoundError:
        log.warning("event: bronze tidak ada utk %s", label)
        return {}

    df = _bersihkan(bronze)
    if df.empty:
        log.warning("event: tidak ada baris valid setelah cleaning")
        return {}

    df["id_event"] = [
        _id_event(r["sumber"], r["judul"], r["tanggal_mulai"], r["url_detail"])
        for _, r in df.iterrows()
    ]
    df = _lengkapi_koordinat(df)
    df = df.drop_duplicates(subset=["id_event"]).reset_index(drop=True)

    if dry_run:
        log.info("[dry-run] event silver: %d baris (tidak ditulis)", len(df))
        return {"agenda": df}

    path = write_parquet(df, "silver", SOURCE, DATASET, label)
    log.info("event silver ditulis: %s (%d baris)", path, len(df))
    return {"agenda": df}