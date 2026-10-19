from __future__ import annotations

import json
import logging
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Optional

import pandas as pd
import requests

from ..shared.config import get_source_config
from ..shared.row_limit import get_row_limit, truncate_df
from ..shared.storage import DATA_LABEL, read_parquet, write_parquet_snapshot

log = logging.getLogger(__name__)

SOURCE = "poi"

BBOX_JAKARTA = {
    "south": -6.37,
    "north": -6.08,
    "west": 106.68,
    "east": 106.99,
}

OVERPASS_HOSTS = [
    "https://overpass.kumi.systems/api/interpreter",                 # Server 1 - Kumi Systems
    "https://overpass-api.de/api/interpreter",                       # Server 2 - Overpass API DE
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",       # Server 3 - Mail.ru
    "https://overpass.openstreetmap.fr/api/interpreter",             # Server 4 - OSM France
    "https://overpass.nchc.org.tw/api/interpreter",                  # Server 5 - NCHC Taiwan
]

# Nama pendek tiap server untuk log ramah (label "Server N")
OVERPASS_NAMA = {
    "https://overpass.kumi.systems/api/interpreter": "Kumi Systems",
    "https://overpass-api.de/api/interpreter": "Overpass API DE",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter": "Mail.ru",
    "https://overpass.openstreetmap.fr/api/interpreter": "OSM France",
    "https://overpass.nchc.org.tw/api/interpreter": "NCHC Taiwan",
}


def _label_server(host: str) -> str:
    idx = OVERPASS_HOSTS.index(host) + 1
    return f"server Overpass {idx} ({OVERPASS_NAMA.get(host, host)})"

# (kategori_utama, tag_osm, key, value) — dari script nyoba_jxb
KATEGORI: list[tuple[str, str, str, str]] = [
    ("F&B", "amenity=restaurant", "amenity", "restaurant"),
    ("F&B", "amenity=cafe", "amenity", "cafe"),
    ("F&B", "amenity=fast_food", "amenity", "fast_food"),
    ("F&B", "amenity=food_court", "amenity", "food_court"),
    ("F&B", "amenity=bar", "amenity", "bar"),
    ("Kebutuhan Harian & Ritel", "shop=convenience", "shop", "convenience"),
    ("Kebutuhan Harian & Ritel", "shop=supermarket", "shop", "supermarket"),
    ("Kebutuhan Harian & Ritel", "amenity=pharmacy", "amenity", "pharmacy"),
    ("Kebutuhan Harian & Ritel", "amenity=atm", "amenity", "atm"),
    ("Kebutuhan Harian & Ritel", "amenity=bank", "amenity", "bank"),
    ("Transportasi Publik", "public_transport=station", "public_transport", "station"),
    ("Transportasi Publik", "highway=bus_stop", "highway", "bus_stop"),
    ("Transportasi Publik", "amenity=bus_station", "amenity", "bus_station"),
    ("Transportasi Publik", "amenity=taxi", "amenity", "taxi"),
    ("Transportasi Publik", "amenity=parking", "amenity", "parking"),
    ("Pusat Perbelanjaan", "shop=mall", "shop", "mall"),
    ("Pusat Perbelanjaan", "shop=department_store", "shop", "department_store"),
    ("Pusat Perbelanjaan", "shop=clothes", "shop", "clothes"),
    ("Pariwisata & Hiburan", "tourism=attraction", "tourism", "attraction"),
    ("Pariwisata & Hiburan", "tourism=museum", "tourism", "museum"),
    ("Pariwisata & Hiburan", "tourism=gallery", "tourism", "gallery"),
    ("Pariwisata & Hiburan", "leisure=park", "leisure", "park"),
    ("Pariwisata & Hiburan", "historic=monument", "historic", "monument"),
    ("Kesehatan & Keamanan", "amenity=hospital", "amenity", "hospital"),
    ("Kesehatan & Keamanan", "amenity=clinic", "amenity", "clinic"),
    ("Kesehatan & Keamanan", "amenity=police", "amenity", "police"),
    ("Layanan Turis Ekstra", "shop=gift", "shop", "gift"),
    ("Layanan Turis Ekstra", "shop=souvenir", "shop", "souvenir"),
    ("Layanan Turis Ekstra", "amenity=bureau_de_change", "amenity", "bureau_de_change"),
    ("Layanan Turis Ekstra", "amenity=cinema", "amenity", "cinema"),
    ("Layanan Turis Ekstra", "amenity=place_of_worship", "amenity", "place_of_worship"),
    ("Layanan Turis Ekstra", "leisure=fitness_centre", "leisure", "fitness_centre"),
    ("Layanan Turis Ekstra", "shop=massage", "shop", "massage"),
    ("Layanan Turis Ekstra", "leisure=spa", "leisure", "spa"),
    ("Layanan Personal", "shop=laundry", "shop", "laundry"),
    ("Layanan Personal", "shop=hairdresser", "shop", "hairdresser"),
    ("Layanan Personal", "shop=beauty", "shop", "beauty"),
    ("Layanan Personal", "shop=travel_agency", "shop", "travel_agency"),
    ("Mobilitas & Sewa", "amenity=car_rental", "amenity", "car_rental"),
    ("Mobilitas & Sewa", "amenity=bicycle_rental", "amenity", "bicycle_rental"),
    ("Hiburan Malam", "amenity=nightclub", "amenity", "nightclub"),
    ("Hiburan Malam", "amenity=theatre", "amenity", "theatre"),
    ("Hiburan Malam", "amenity=karaoke", "amenity", "karaoke"),
    ("Acara & Olahraga", "leisure=stadium", "leisure", "stadium"),
    ("Acara & Olahraga", "leisure=sports_centre", "leisure", "sports_centre"),
    ("Acara & Olahraga", "amenity=events_venue", "amenity", "events_venue"),
    ("Pemerintahan & Diplomatik", "amenity=embassy", "amenity", "embassy"),
    ("Pemerintahan & Diplomatik", "office=government", "office", "government"),
    ("Infrastruktur Kendaraan", "amenity=fuel", "amenity", "fuel"),
    ("Infrastruktur Kendaraan", "amenity=charging_station", "amenity", "charging_station"),
    ("Pendidikan Tinggi", "amenity=university", "amenity", "university"),
    ("Kuliner Spesifik", "shop=bakery", "shop", "bakery"),
    ("Kuliner Spesifik", "amenity=ice_cream", "amenity", "ice_cream"),
    ("Kuliner Spesifik", "shop=coffee", "shop", "coffee"),
]

OFFSET_PENGGANTI = {"node": 0, "way": 1_000_000_000_000, "relation": 2_000_000_000_000}

WAKTU_JEDA_OVERPASS = 3  # detik antar kategori (endpoint publik membatasi rate)


def _bbox_teks() -> str:
    return (
        f"{BBOX_JAKARTA['south']},{BBOX_JAKARTA['west']},"
        f"{BBOX_JAKARTA['north']},{BBOX_JAKARTA['east']}"
    )


def _bangun_query_grup(key: str, values: list[str]) -> str:
    """Satu query Overpass untuk sekelompok nilai tag (union regex per key).

    Dipakai agar 52 kategori TIDAK memicu 52 koneksi serial ke server
    (sumber utama 504/read-timeout). Grouping per key -> total ~8 query,
    memangkas paparan ke mirror publik yang tidak stabil.
    """
    pola = "|".join(re.escape(v) for v in values)
    return (
        '[out:json][timeout:180];\n'
        f'nwr["{key}"~"^({pola})$"]({_bbox_teks()});\nout center;'
    )


def _kelompok_kategori() -> dict[str, list[str]]:
    """key OSM -> daftar nilai yang dicari dalam satu query regex."""
    kelompok: dict[str, list[str]] = {}
    for _, _, key, value in KATEGORI:
        if value not in kelompok.setdefault(key, []):
            kelompok[key].append(value)
    return kelompok


def _ambil_overpass(query: str, rounds: int = 3) -> tuple[list[dict], bool]:
    """Coba tiap host Overpass bergantian; hormati 429 (Retry-After).

    Mengembalikan (elemen, ok): ok=True bila setidaknya satu server berhasil
    menjawab (meski elemen kosong = memang tidak ada data); ok=False hanya bila
    SEMUA server gagal di semua percobaan. Kecuali 429 (rate limit), semua
    kegagalan transien satu host hanya dicatat level INFO agar konsol tidak
    dibanjiri peringatan 50x; peringatan per tag ditangani oleh `extract()`.

    Serupa skrip lama nyoba_jxb yang terbukti stabil: read timeout mengikuti
    batas server ([out:json][timeout:180]) dan backoff lebar (8s per percobaan)
    agar tidak menyemprot mirror publik. Urutan host diacak per panggilan agar
    beban tersebar (tidak semua request masuk ke host pertama).
    """
    for percobaan in range(1, rounds + 1):
        urutan = list(OVERPASS_HOSTS)
        random.shuffle(urutan)
        for host in urutan:
            label = _label_server(host)
            try:
                resp = requests.post(
                    host,
                    data={"data": query},
                    headers={"User-Agent": "JXB_DSS_Pipeline/1.0"},
                    timeout=(20, 200),
                )
                if resp.status_code == 200:
                    elemen = resp.json().get("elements", [])
                    log.info("poi ok via %s (%d elemen)", _label_server(host), len(elemen))
                    return elemen, True
                if resp.status_code == 429:
                    tunggu = min(int(resp.headers.get("Retry-After", 10)), 20)
                    log.info("poi: %s rate limit (429); tunggu %ss", label, tunggu)
                    time.sleep(tunggu)
                    continue
                log.info("poi: %s gagal (HTTP %s); coba server lain...", label, resp.status_code)
            except (requests.RequestException, ValueError) as exc:
                log.info("poi: %s gagal (percobaan %s): %s; coba server lain...", label, percobaan, exc)
        time.sleep(8 * percobaan)
    return [], False


def _kategorikan(tags: dict) -> tuple[str, str, tuple[str, str]] | None:
    for kategori, tag, key, value in KATEGORI:
        if tags.get(key) == value:
            return kategori, tag, (key, value)
    return None


def _transformasi_element(el: dict) -> Optional[dict[str, Any]]:
    tags = el.get("tags") or {}
    hasil = _kategorikan(tags)
    if not hasil:
        return None
    kategori, tag_osm, (kunci, nilai) = hasil

    tipe = el.get("type")
    if tipe == "node":
        lat, lon = el.get("lat"), el.get("lon")
    else:
        pusat = el.get("center") or {}
        lat, lon = pusat.get("lat"), pusat.get("lon")
    if lat is None or lon is None:
        return None

    nama = tags.get("name") or tags.get("brand") or tags.get("operator")

    metadata = dict(tags)
    metadata.pop("name", None)
    metadata.pop(kunci, None)
    metadata["tipe_osm"] = tipe

    return {
        "id_osm": el["id"] + OFFSET_PENGGANTI.get(tipe, 0),
        "nama_poi": nama,
        "kategori_utama": kategori,
        "tag_osm": tag_osm,
        "latitude": round(float(lat), 8),
        "longitude": round(float(lon), 8),
        "metadata": json.dumps(metadata, ensure_ascii=False),
    }


def _gabung_dengan_bronze_sebelumnya(
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Gabungkan hasil baru dengan snapshot bronze sebelumnya (dedup id_osm).

    Baris baru selalu diutamakan; baris lama dipertahankan sehingga run yang
    hanya berhasil sebagian (grup Overpass gagal) TIDAK menghapus/ mengurangi
    snapshot yang sudah tersimpan. POI Overpass hampir statis antar run, jadi
    merge aditif aman.
    """
    try:
        lama = read_parquet("bronze", SOURCE, "poi", DATA_LABEL)
    except FileNotFoundError:
        return rows
    if lama is None or lama.empty:
        return rows
    gabung = pd.concat([pd.DataFrame(rows), lama], ignore_index=True)
    gabung = gabung.drop_duplicates(subset=["id_osm"], keep="first").reset_index(drop=True)
    return gabung.to_dict("records")


def extract(period: str | None = None, dry_run: bool = False) -> dict[str, pd.DataFrame]:
    """EXTRACT (bronze): POI DKI Jakarta dari Overpass API.

    Query digabung per key OSM (union regex) sehingga hanya ~8 koneksi, bukan
    52; run yang berhasil sebagian digabung dengan snapshot bronze sebelumnya
    agar data tidak berkurang. period/tanggal tidak berlaku untuk Overpass
    (data snapshot); bronze ditulis sebagai satu file penuh `data.parquet`
    (grain: snapshot).
    """
    cfg = get_source_config(SOURCE)

    rows: list[dict[str, Any]] = []
    seen: set[int] = set()
    jumlah_gagal = 0

    pekerja: dict[int, tuple[str, list[str]]] = {}
    for i, (key, values) in enumerate(_kelompok_kategori().items()):
        pekerja[i] = (key, values)

    def _kerjakan(item: tuple[int, tuple[str, list[str]]]) -> tuple[list[dict[str, Any]], bool]:
        _, (key, values) = item
        elemen, ok = _ambil_overpass(_bangun_query_grup(key, values))
        baris = []
        kunci_dilihat: set[tuple[str, int]] = set()
        for el in elemen:
            r = _transformasi_element(el)
            if not r:
                continue
            kunci_unik = (el.get("type"), el["id"])
            if kunci_unik in kunci_dilihat:
                continue
            kunci_dilihat.add(kunci_unik)
            baris.append(r)
        return baris, ok

    # Serial default (1 worker) seperti pola lama yang stabil; naikkan lewat
    # config (poi.overpass_workers) jika mirror dirasa sanggup menangani.
    limit = get_row_limit()
    with ThreadPoolExecutor(max_workers=int(cfg.get("overpass_workers", 1))) as ex:
        futures = {ex.submit(_kerjakan, item): item for item in pekerja.items()}
        for fut in as_completed(futures):
            item = futures[fut]
            key, values = item[1]
            try:
                baris, ok = fut.result()
                if not ok:
                    jumlah_gagal += len(values)
                    for v in values:
                        log.warning(
                            "poi: semua %d server Overpass gagal utk tag '%s=%s' "
                            "(snapshot lama dipertahankan)",
                            len(OVERPASS_HOSTS), key, v,
                        )
                elif not baris:
                    log.info("poi: grup '%s' tanpa elemen (data memang kosong)", key)
                for r in baris:
                    if r["id_osm"] in seen:
                        continue
                    seen.add(r["id_osm"])
                    rows.append(r)
                    if limit and len(rows) >= limit:
                        break
            except Exception as exc:
                jumlah_gagal += len(values)
                log.error("poi kategori gagal: %s", exc)
            if limit and len(rows) >= limit:
                log.info("poi: cap %d baris tercapai; kategori tersisa dibatalkan", limit)
                for f in futures:
                    f.cancel()
                break
            time.sleep(WAKTU_JEDA_OVERPASS)

    if not rows:
        log.warning("poi: tidak ada POI yang ditemukan")
        return {}

    if jumlah_gagal:
        total_tag = sum(len(v) for v in pekerja.values())
        log.warning(
            "poi: %d dari %d tag kategori gagal diambil; snapshot sebelumnya dipertahankan",
            jumlah_gagal,
            total_tag,
        )

    rows_final = rows if dry_run else _gabung_dengan_bronze_sebelumnya(rows)
    df = pd.DataFrame(rows_final)
    df["tanggal_tarik"] = pd.Timestamp.now().date().isoformat()
    df = truncate_df(df)

    if dry_run:
        log.info("[dry-run] poi bronze: %d baris (tidak ditulis)", len(df))
        return {"poi": df}

    path = write_parquet_snapshot(df, "bronze", SOURCE, "poi")
    log.info("poi bronze ditulis: %s (%d baris)", path, len(df))
    return {"poi": df}