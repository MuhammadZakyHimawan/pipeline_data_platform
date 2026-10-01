from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, timedelta

import pandas as pd

from ..shared.periods import resolve_years
from ..shared.storage import list_periods, read_parquet, write_parquet

log = logging.getLogger(__name__)

SOURCE = "hari_libur"


def _deteksi_kejepit(semua_tanggal: list[date], set_libur: set[date]) -> tuple[set[date], set[date]]:
    base_off = set_libur | {d for d in semua_tanggal if d.weekday() >= 5}
    kejepit = set()
    for d in semua_tanggal:
        if d in set_libur or d.weekday() >= 5:
            continue
        if (d - timedelta(days=1)) in base_off and (d + timedelta(days=1)) in base_off:
            kejepit.add(d)
    return base_off, kejepit


def _deteksi_rentang_libur(semua_tanggal: list[date], set_non_kerja: set[date]) -> list[list[date]]:
    runs: list[list[date]] = []
    current: list[date] = []
    for d in semua_tanggal:
        if d in set_non_kerja:
            current.append(d)
        else:
            if current:
                runs.append(current)
            current = []
    if current:
        runs.append(current)
    return [r for r in runs if len(r) >= 3]


def _kunci_keterangan(d: date, info: dict, kejepit: set[date], run: list[date], run_len: int) -> str:
    if d in info:
        h = info[d]
        tipe = "Cuti Bersama" if h["cuti_bersama"] else "Libur Nasional"
        teks = f"{tipe}: {h['name']}"
        if run_len >= 3:
            teks += f" (libur panjang {run_len} hari)"
        return teks
    if d in kejepit:
        nama_di_run = list(dict.fromkeys(info[x]["name"] for x in run if x in info))
        sambungan = ", ".join(nama_di_run) if nama_di_run else "hari libur"
        return f"Hari Kejepit - disarankan ambil cuti (libur panjang {run_len} hari, rangkaian: {sambungan})"
    nama_di_run = list(dict.fromkeys(info[x]["name"] for x in run if x in info))
    sambungan = ", ".join(nama_di_run) if nama_di_run else "hari libur"
    return f"Libur Panjang {run_len} hari - rangkaian: {sambungan}"


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["tanggal"] = pd.to_datetime(df["tanggal"]).dt.date
    df = df.drop_duplicates(subset=["tanggal"])

    # hanya event "Hari libur nasional" (sesuai script lama)
    mask_nasional = df["deskripsi"].fillna("").str.contains("Hari libur nasional")
    libur_rows = df[mask_nasional]

    info: dict[date, dict] = {}
    for _, r in libur_rows.iterrows():
        d = r["tanggal"]
        if d not in info:
            info[d] = {
                "name": r["nama"],
                "cuti_bersama": str(r["nama"]).lower().startswith("cuti bersama"),
            }

    if not info:
        log.warning("hari_libur: tidak ada event libur nasional terfilter")
        return pd.DataFrame()

    tahun_mulai = min(info.keys()).year
    tahun_akhir = max(info.keys()).year
    mulai = date(tahun_mulai, 1, 1)
    akhir = date(tahun_akhir, 12, 31)
    semua_tanggal = [mulai + timedelta(days=i) for i in range((akhir - mulai).days + 1)]

    set_libur = set(info)
    _, kejepit = _deteksi_kejepit(semua_tanggal, set_libur)
    non_kerja = set_libur | kejepit | {d for d in semua_tanggal if d.weekday() >= 5}
    runs = _deteksi_rentang_libur(semua_tanggal, non_kerja)

    run_by_tanggal: dict[date, list[date]] = {d: run for run in runs for d in run}
    akan_insert = set_libur | kejepit | {d for run in runs for d in run}

    rows = []
    for d in sorted(akan_insert):
        run = run_by_tanggal.get(d, [d])
        run_len = len(run)
        h = info.get(d)
        rows.append(
            {
                "id": d.isoformat(),
                "tanggal": d,
                "is_weekend": d.weekday() >= 5,
                "is_libur_nasional": bool(h and not h["cuti_bersama"]),
                "is_cuti_bersama": bool(h and h["cuti_bersama"]),
                "is_libur_sekolah": False,
                "is_kejepit": d in kejepit,
                "is_libur_panjang": run_len >= 3,
                "total_hari_beruntun": run_len,
                "keterangan": _kunci_keterangan(d, info, kejepit, run, run_len),
            }
        )
    return pd.DataFrame(rows)


def transform(period: str | None = None, dry_run: bool = False) -> dict[str, pd.DataFrame]:
    """TRANSFORM (silver): seluruh file tahun hari libur -> silver per tahun."""
    periods = (
        [str(y) for y in resolve_years(period)]
        if period
        else list_periods("bronze", SOURCE, "hari_libur")
    )
    if not periods:
        log.warning("hari_libur: tidak ada file bronze untuk diproses")
        return {}

    frames: list[pd.DataFrame] = []
    for p in periods:
        try:
            bronze = read_parquet("bronze", SOURCE, "hari_libur", p)
        except FileNotFoundError:
            log.warning("hari_libur: bronze tidak ada utk periode %s", p)
            continue
        df = _clean(bronze)
        if df.empty:
            log.warning("hari_libur: hasil clean kosong utk periode %s", p)
            continue
        if dry_run:
            log.info("[dry-run] hari_libur silver %s: %d baris (tidak ditulis)", p, len(df))
        else:
            path = write_parquet(df, "silver", SOURCE, "hari_libur", p)
            log.info("hari_libur silver ditulis: %s (%d baris)", path, len(df))
        frames.append(df)

    if not frames:
        return {}
    return {"hari_libur": pd.concat(frames, ignore_index=True)}