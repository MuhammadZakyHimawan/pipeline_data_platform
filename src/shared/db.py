from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterator, Sequence

import pandas as pd
import psycopg2
import psycopg2.extras

from .config import get_db_config
from .row_limit import truncate_df

log = logging.getLogger(__name__)


def build_conn_params() -> dict:
    db = get_db_config()
    return {
        "host": db["host"],
        "port": db["port"],
        "dbname": db["name"],
        "user": db["user"],
        "password": db["pass"],
        "connect_timeout": 10,
    }


@contextmanager
def db_connection() -> Iterator:
    conn = psycopg2.connect(**build_conn_params())
    try:
        yield conn
    finally:
        conn.close()


def ensure_schema(schema: str) -> None:
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        conn.commit()


def table_exists(schema: str, table: str) -> bool:
    """True bila {schema}.{table} sudah ada (tidak melempar error bila tidak)."""
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", (f'"{schema}"."{table}"',))
            return cur.fetchone()[0] is not None


def fetch_df(sql: str, params: Sequence | None = None) -> pd.DataFrame | None:
    """Jalankan query SELECT dan kembalikan hasilnya sebagai DataFrame.

    None bila tidak ada baris. purposely memakai cursor + DataFrame (bukan
    pd.read_sql) supaya tidak butuh SQLAlchemy yang belum terpasang di proyek
    ini, dan tidak memunculkan warning pandas untuk koneksi DBAPI2.
    """
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if cur.description is None:
                return None
            kolom = [d[0] for d in cur.description]
            baris = cur.fetchall()
    if not baris:
        return None
    return pd.DataFrame(baris, columns=kolom)


def _to_pg_type(dtype: str) -> str:
    d = str(dtype).lower()
    if "int" in d:
        return "BIGINT"
    if "float" in d or "double" in d:
        return "DOUBLE PRECISION"
    if "datetime" in d or "timestamp" in d:
        return "TIMESTAMP"
    if d == "date":
        return "DATE"
    if "bool" in d:
        return "BOOLEAN"
    return "TEXT"


def build_table_sql(schema: str, table: str, columns: dict[str, str], primary_key: Sequence[str]) -> str:
    col_defs = ", ".join(f'"{col}" {_to_pg_type(dtype)}' for col, dtype in columns.items())
    if primary_key:
        pk_list = ", ".join(f'"{c}"' for c in primary_key)
        col_defs += f", PRIMARY KEY ({pk_list})"
    return f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" ({col_defs})'


def upsert_df(
    df,
    schema: str,
    table: str,
    conflict_keys: Sequence[str],
    create_table_on_first: bool = True,
) -> int:
    """Upsert DataFrame ke {schema}.{table} via CREATE TABLE IF NOT EXISTS + INSERT ... ON CONFLICT."""
    if df is None or df.empty:
        log.warning("upsert skipped: df kosong (%s.%s)", schema, table)
        return 0

    if not conflict_keys:
        raise ValueError("conflict_keys tidak boleh kosong untuk upsert")

    df = truncate_df(df)
    if df is None or df.empty:
        log.warning("upsert skipped: df kosong setelah dibatasi (%s.%s)", schema, table)
        return 0

    df = df.reset_index(drop=True)
    columns = {col: str(df[col].dtype) for col in df.columns}
    table_sql = build_table_sql(schema, table, columns, conflict_keys)
    records = df.to_dict(orient="records")

    ensure_schema(schema)

    if create_table_on_first:
        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(table_sql)
            conn.commit()

    col_names = list(columns.keys())
    col_placeholders = ", ".join(["%s"] * len(col_names))
    quoted_cols = ", ".join(f'"{c}"' for c in col_names)
    conflict_cols = ", ".join(f'"{c}"' for c in conflict_keys)
    updates = [f'"{c}" = EXCLUDED."{c}"' for c in col_names if c not in set(conflict_keys)]

    insert_sql = (
        f'INSERT INTO "{schema}"."{table}" ({quoted_cols}) VALUES ({col_placeholders}) '
        f"ON CONFLICT ({conflict_cols}) "
    )
    if updates:
        insert_sql += "DO UPDATE SET " + ", ".join(updates)
    else:
        insert_sql += "DO NOTHING"

    with db_connection() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(
                cur,
                insert_sql,
                [[r.get(c) for c in col_names] for r in records],
                page_size=1000,
            )
        conn.commit()

    log.info("upsert %s.%s: %d rows", schema, table, len(records))
    return len(records)


def delete_scope(
    schema: str,
    table: str,
    condition_col: str,
    value,
) -> int:
    """Hapus baris dengan nilai kolom tertentu (mis. hapus period lama sebelum insert ulang)."""
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f'DELETE FROM "{schema}"."{table}" WHERE "{condition_col}" = %s',
                (value,),
            )
            affected = cur.rowcount
        conn.commit()
    return affected