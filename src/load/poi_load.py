from __future__ import annotations

import logging

import pandas as pd

from ..shared.config import get_db_config
from ..shared.db import db_connection, upsert_df
from ..shared.row_limit import is_limited
from ..shared.storage import read_parquet

log = logging.getLogger(__name__)

SOURCE = "poi"
TABLE = "poi_master"
CONFLICT_KEYS = ["id_osm"]

_KOLOM_ABAIKAN = {"tanggal_tarik"}


def _norm(v) -> str:
    """Normalisasi nilai untuk pembandingan: None/NaN/pd.NA dianggap kosong."""
    return "" if pd.isna(v) else str(v)


def _delta_terhadap_db(df: pd.DataFrame, schema: str, table: str) -> tuple[pd.DataFrame, int, int]:
    """Bandingkan df vs tabel (gold.poi_master) per id_osm pada kolom nilai.

    Mengembalikan (df_delta, jumlah_baru, jumlah_berubah). Baris yang sama
    persis (kecuali tanggal_tarik) di-skip supaya run tanpa perubahan tidak
    melakukan INSERT. Tabel yang belum ada dianggap kosong (semua "baru").
    """
    cols = [c for c in df.columns if c not in _KOLOM_ABAIKAN]

    sig_db: dict[int, tuple[str, ...]] = {}
    with db_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT to_regclass(%s)", (f'"{schema}"."{table}"',))
        ada = cur.fetchone()[0]
        if ada:
            quoted = ", ".join(f'"{c}"' for c in cols)
            cur.execute(f'SELECT "id_osm", {quoted} FROM "{schema}"."{table}"')
            for row in cur.fetchall():
                sig_db[row[0]] = tuple(_norm(v) for v in row[1:])

    index_baru: list[int] = []
    index_berubah: list[int] = []
    for idx, row in df.iterrows():
        sig = tuple(_norm(row[c]) for c in cols)
        if row["id_osm"] not in sig_db:
            index_baru.append(idx)
        elif sig_db[row["id_osm"]] != sig:
            index_berubah.append(idx)

    delta = df.loc[index_baru + index_berubah].reset_index(drop=True)
    return delta, len(index_baru), len(index_berubah)


def load(period: str | None = None, dry_run: bool = False) -> dict[str, pd.DataFrame]:
    label = (period or "latest").strip().lower()

    try:
        df = read_parquet("silver", SOURCE, "poi", label)
    except FileNotFoundError:
        log.warning("poi: silver tidak ada utk %s", label)
        return {}

    if df.empty:
        return {}

    schema = get_db_config()["schema_gold"]
    if dry_run:
        log.info("[dry-run] poi -> %s.%s (%d baris, tidak dimuat)", schema, TABLE, len(df))
        return {"poi": df}

    if is_limited():
        log.info("%d data %s berhasil didapatkan", len(df), SOURCE)
        return {"poi": df}

    delta, baru, berubah = _delta_terhadap_db(df, schema, TABLE)
    if delta.empty:
        log.info(
            "poi: tidak ada data baru/perubahan (%d baru, %d berubah) -> 0 baris di-insert",
            baru,
            berubah,
        )
        return {"poi": delta}

    upsert_df(delta, schema=schema, table=TABLE, conflict_keys=CONFLICT_KEYS)
    log.info(
        "poi: %d baru, %d berubah di-insert ke %s.%s",
        baru,
        berubah,
        schema,
        TABLE,
    )
    return {"poi": delta}