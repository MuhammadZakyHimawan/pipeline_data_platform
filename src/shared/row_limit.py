from __future__ import annotations


# Dua state yang sengaja dipisah: `row_limit` membatasi unit kerja (berapa
# hotel, rute, atau baris yang diproses), `check` menahan semua write. Semula
# keduanya hidup dari satu `_limit`, sehingga `sumber[N]` berarti "batasi
# unit" sekaligus "jangan tulis" -- akibatnya run uji tidak pernah menghasilkan
# parquet, padahal tujuan uji justru memastikan parquet terbentuk.
_limit: int | None = None
_check: bool = False


def set_row_limit(n: int | None) -> None:
    """Set batas unit global. N < 1 atau None = tanpa batas."""
    global _limit
    if n is not None:
        n = int(n)
        if n < 1:
            n = None
    _limit = n


def get_row_limit() -> int | None:
    """Batas unit aktif (None = tanpa batas / run penuh)."""
    return _limit


def set_check(nilai: bool) -> None:
    """Aktifkan mode check: jalankan penuh tapi jangan menulis file/DB apa pun."""
    global _check
    _check = bool(nilai)


def is_check() -> bool:
    """True bila mode check aktif: semua penulisan disk dan DB dilewati."""
    return _check


def is_limited() -> bool:
    """True bila batas unit (row limit) aktif. Hanya informasi, TIDAK menghentikan write."""
    return _limit is not None


def set_test() -> None:
    """Mode test: batasi 3 unit, tetap tulis output (check = False)."""
    set_row_limit(3)
    global _check
    _check = False


def truncate_df(df):
    """Potong df ke batas unit aktif (None = kembalikan apa adanya)."""
    if _limit is not None and df is not None:
        return df.head(_limit)
    return df