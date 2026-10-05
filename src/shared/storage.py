from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Callable

import pandas as pd

from .config import DATA_LAKE_DIR
from .row_limit import is_check

log = logging.getLogger(__name__)

# Label file untuk sumber snapshot (tanpa dimensi waktu).
DATA_LABEL = "data"

# Kolom tanggal yang lazim dipakai tiap sumber untuk diturunkan tahun filet.
DATE_COLUMNS = ("tanggal", "time", "tanggal_mulai", "tanggal_berita", "tanggal_tarik")


def _dataset_dir(layer: str, source: str, dataset: str) -> Path:
    return DATA_LAKE_DIR / layer / source / dataset


def _hitung_rows(path: Path) -> int:
    """Hitung jumlah baris file parquet; 0 jika tidak ada/error."""
    try:
        if path.exists():
            return len(pd.read_parquet(path))
    except Exception:
        pass
    return 0


def _tulis(path: Path, df: pd.DataFrame) -> tuple[Path, bool]:
    """Tulis DataFrame ke `path`; kembalikan (path, benar_ditulis).

    Status kedua penting: mode uji sengaja tidak menyentuh disk, jadi pemanggil
    perlu bisa membedakan "file ditulis" dari "file dilewati" supaya laporan run
    tidak menyatakan berhasil menulis padahal nol byte tersimpan.
    """
    if is_check():
        log.warning(
            "mode check aktif: TIDAK menulis %s (--check menahan penulisan)",
            path.name,
        )
        return path, False

    rows_sebelum = _hitung_rows(path)

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        df.to_parquet(tmp, index=False)
        os.replace(tmp, path)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise

    rows_sesudah = _hitung_rows(path)
    diff = rows_sesudah - rows_sebelum
    tanda = "+" if diff >= 0 else ""
    log.info(
        "%s: %d → %d baris (%s%d)",
        path.relative_to(DATA_LAKE_DIR),
        rows_sebelum,
        rows_sesudah,
        tanda,
        diff,
    )
    return path, True


def _cari_kolom_tahun(df: pd.DataFrame, year_col: str | None) -> str | None:
    """Cari kolom yang bisa diturunkan ke tahun file."""
    if year_col and year_col in df.columns:
        return year_col
    for col in DATE_COLUMNS:
        if col in df.columns:
            return col
    return None


def _tahun_dari(seri: pd.Series) -> pd.Series:
    """Turunkan tahun dari kolom tahun (int) atau kolom tanggal (datetime/str)."""
    if pd.api.types.is_datetime64_any_dtype(seri):
        return seri.dt.year
    if pd.api.types.is_integer_dtype(seri) or pd.api.types.is_float_dtype(seri):
        return seri.astype("Int64")
    return pd.to_datetime(seri, errors="coerce").dt.year


def write_parquet(
    df: pd.DataFrame,
    layer: str,
    source: str,
    dataset: str,
    period: str,
) -> Path:
    """Tulis DataFrame ke data_lake/{layer}/{source}/{dataset}/{period}.parquet"""
    if df is None or df.empty:
        raise ValueError(f"tidak ada data untuk ditulis: {layer}/{source}/{dataset}/{period}")
    path = _dataset_dir(layer, source, dataset) / f"{period}.parquet"
    return _tulis(path, df)[0]


def write_parquet_by_year(
    df: pd.DataFrame,
    layer: str,
    source: str,
    dataset: str,
    year_col: str | None = None,
    mode: str = "replace",
    dedup_cols: list[str] | None = None,
) -> list[Path]:
    """Pecah DataFrame menjadi satu file per tahun: {dataset}/{YYYY}.parquet.

    Args:
        year_col: kolom sumber tahun/tanggal. Defaults: 'tahun' lalu kolom
            tanggal yang lazim (tanggal, time, tanggal_mulai, ...).
        mode:
            'replace' - file tahun ditimpa penuh oleh data run ini (default,
                dipakai sumber yang(fetch ulang satu periode utuh).
            'append'  - baris baru digabung ke file tahun yang sudah ada
                (dipakai feed bertahap seperti gdelt, agar akumulatif tanpa
                duplikasi).
        dedup_cols: kolom kunci unik baris untuk mode='append'. Default: semua
            kolom. Hanya dipakai bila file tahun sudah ada.

    Returns:
        List path yang ditulis (urut berdasarkan tahun).
    """
    if df is None or df.empty:
        raise ValueError(f"tidak ada data untuk ditulis: {layer}/{source}/{dataset}")

    col = _cari_kolom_tahun(df, year_col)
    if col is None:
        raise ValueError(
            f"tidak ada kolom tahun/tanggal untuk {layer}/{source}/{dataset}; "
            f"kolom tersedia: {list(df.columns)}"
        )

    tahun = _tahun_dari(df[col])
    if tahun.isna().all():
        raise ValueError(
            f"kolom '{col}' {layer}/{source}/{dataset} tidak menghasilkan tahun yang valid"
        )

    paths: list[Path] = []
    n_ditulis = 0
    n_dilewati = 0
    d = _dataset_dir(layer, source, dataset)
    for y in sorted(tahun.dropna().unique().tolist()):
        bagian = df[tahun == y]
        if bagian.empty:
            continue
        path = d / f"{int(y)}.parquet"
        if mode == "append" and path.exists():
            lama = pd.read_parquet(path)
            bagian = pd.concat([lama, bagian], ignore_index=True)
            kunci = dedup_cols or list(bagian.columns)
            kunci = [k for k in kunci if k in bagian.columns]
            if kunci:
                bagian = bagian.drop_duplicates(subset=kunci, keep="last")
        hasil_tulis = _tulis(path, bagian)
        paths.append(hasil_tulis[0])
        if hasil_tulis[1]:
            n_ditulis += 1
        else:
            n_dilewati += 1

    # Laporan dipisah "ditulis" vs "dilewati": pada mode uji semua file dilewati,
    # jadi kalimat "ditulis" saja menyesatkan.
    catatan = ""
    if n_dilewati and n_ditulis:
        catatan = f" ({n_ditulis} ditulis, {n_dilewati} dilewati)"
    elif n_dilewati:
        catatan = f" ({n_ditulis} ditulis, {n_dilewati} dilewati mode check)"
    log.info(
        "%s/%s: %d file per tahun%s (%s..%s)",
        layer,
        source,
        len(paths),
        catatan,
        paths[0].stem if paths else "-",
        paths[-1].stem if paths else "-",
    )
    return paths


def write_parquet_snapshot(
    df: pd.DataFrame,
    layer: str,
    source: str,
    dataset: str,
) -> Path:
    """Tulis satu file penuh tanpa waktu: {dataset}/data.parquet."""
    if df is None or df.empty:
        raise ValueError(f"tidak ada data untuk ditulis: {layer}/{source}/{dataset}/{DATA_LABEL}")
    path = _dataset_dir(layer, source, dataset) / f"{DATA_LABEL}.parquet"
    return _tulis(path, df)[0]


def read_parquet(
    layer: str,
    source: str,
    dataset: str,
    period: str,
) -> pd.DataFrame:
    path = _dataset_dir(layer, source, dataset) / f"{period}.parquet"
    if not path.exists():
        raise FileNotFoundError(f"parquet tidak ditemukan: {path}")
    return pd.read_parquet(path)


def list_periods(layer: str, source: str, dataset: str) -> list[str]:
    d = _dataset_dir(layer, source, dataset)
    if not d.exists():
        return []
    return sorted(p.stem for p in d.glob("*.parquet"))


def read_all_periods(
    layer: str,
    source: str,
    dataset: str,
    periods: list[str] | None = None,
    on_error: Callable[[str, Exception], None] | None = None,
) -> pd.DataFrame | None:
    """Baca & gabungkan seluruh file periode sebuah dataset.

    Dipakai transform/load agar tidak perlu menebak label periode: semua file
    yang benar-benar ada di data_lake ikut diproses (satu tahun = satu file).
    """
    daftar = periods if periods is not None else list_periods(layer, source, dataset)
    frames: list[pd.DataFrame] = []
    for period in daftar:
        try:
            frames.append(read_parquet(layer, source, dataset, period))
        except FileNotFoundError as exc:
            if on_error is not None:
                on_error(period, exc)
    if not frames:
        return None
    return pd.concat(frames, ignore_index=True)
