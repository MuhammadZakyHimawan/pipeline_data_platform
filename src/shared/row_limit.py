from __future__ import annotations


_limit: int | None = None


def set_row_limit(n: int | None) -> None:
    """Set batas baris global (mode uji). N < 1 atau None = tanpa batas."""
    global _limit
    if n is not None:
        n = int(n)
        if n < 1:
            n = None
    _limit = n


def get_row_limit() -> int | None:
    """Batas baris aktif (None = tanpa batas / run penuh)."""
    return _limit


def is_limited() -> bool:
    """True bila mode uji aktif (baris dibatasi & LOAD tidak menulis ke DB)."""
    return _limit is not None


def truncate_df(df):
    """Potong df ke batas baris aktif (None = kembalikan apa adanya)."""
    if _limit is not None and df is not None:
        return df.head(_limit)
    return df