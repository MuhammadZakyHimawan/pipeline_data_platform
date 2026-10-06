from __future__ import annotations

import logging

import pandas as pd

from ..shared.config import get_db_config, get_source_config
from ..shared.db import db_connection, upsert_df
from ..shared.storage import read_parquet

log = logging.getLogger(__name__)

SOURCE = "transport"
LABEL_DEFAULT = "latest"

# urutan delete: anak -> induk (aman jika ada FK di masa depan)
TABLES: list[tuple[str, str, list[str]]] = [
    # (dataset_silver, tabel_gold, conflict_keys)
    ("pemetaan", "transport_pemetaan_rute_halte", ["nama_rute", "urutan_berhenti"]),
    ("rute", "transport_rute_angkutan", ["nama_rute"]),
    ("titik", "transport_titik_berhenti", ["nama_titik"]),
    ("jenis", "transport_jenis_transportasi", ["nama_jenis"]),
]


def _label(cfg: dict, period: str | None) -> str:
    return (period or cfg.get("period", LABEL_DEFAULT)).strip().lower()


def _clear_tables(schema: str) -> None:
    """Hapus seluruh baris transport (full-reload) mengikuti perilaku referensi."""
    with db_connection() as conn:
        with conn.cursor() as cur:
            for _, table, _ in TABLES:
                cur.execute(f'DELETE FROM "{schema}"."{table}"')
        conn.commit()


def load(period: str | None = None, dry_run: bool = False) -> dict[str, pd.DataFrame]:
    cfg = get_source_config(SOURCE)
    label = _label(cfg, period)

    frames: dict[str, pd.DataFrame] = {}
    for dataset, _, _ in TABLES:
        try:
            df = read_parquet("silver", SOURCE, dataset, label)
        except FileNotFoundError:
            log.warning("transport: silver %s tidak ada utk %s", dataset, label)
            return {}
        frames[dataset] = df

    if any(f.empty for f in frames.values()):
        log.warning("transport: ada silver kosong; load dibatalkan")
        return {}

    schema = get_db_config()["schema_gold"]
    if dry_run:
        for dataset, table, _ in TABLES:
            log.info(
                "[dry-run] transport -> %s.%s (%d baris, tidak dimuat)",
                schema,
                table,
                len(frames[dataset]),
            )
        return frames

    _clear_tables(schema)

    # urutan insert: induk -> anak
    for dataset, table, keys in sorted(
        TABLES, key=lambda t: ["jenis", "rute", "titik", "pemetaan"].index(t[0])
    ):
        upsert_df(frames[dataset], schema=schema, table=table, conflict_keys=keys)
        log.info("%d data %s berhasil dimasukkan ke database dalam tabel %s.%s",
                 len(frames[dataset]), dataset, schema, table)
    return frames