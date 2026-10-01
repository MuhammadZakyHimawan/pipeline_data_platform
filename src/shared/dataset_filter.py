from __future__ import annotations


_datasets: tuple[str, ...] | None = None


def set_dataset_filter(names: tuple[str, ...] | list[str] | None) -> None:
    """Batasi dataset mana yang boleh diproses/diambil.

    None atau kosong = semua dataset (perilaku bawaan). Nilai diisi dari CLI
    seperti `--harga` supaya sumber hanya meng-scrape dataset itu.
    """
    global _datasets
    if not names:
        _datasets = None
    else:
        _datasets = tuple(dict.fromkeys(str(n).strip().lower() for n in names if str(n).strip()))


def get_dataset_filter() -> tuple[str, ...] | None:
    """Filter dataset aktif (None = semua dataset)."""
    return _datasets


def dataset_terpilih() -> str:
    """Filter aktif sebagai teks untuk log: 'semua' atau 'harga, review'."""
    return ", ".join(_datasets) if _datasets else "semua"


def wants(dataset: str) -> bool:
    """True bila dataset ini boleh diproses pada run saat ini."""
    if _datasets is None:
        return True
    return str(dataset).strip().lower() in _datasets
