from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime

from .config import DATA_LAKE_DIR

log = logging.getLogger(__name__)

# State run disimpan terpisah dari parquet: parquet ditulis per periode, sedangkan
# checkpoint dipakai untuk melanjutkan daftar item. Folder ini ikut gitignored
# bersama data_lake/.
DIR_STATE = DATA_LAKE_DIR / "_state"
# Default save threshold (can be overridden per instance)
_SIMPAN_SETIAP_DEFAULT = 10

# Global registry for signal handler access
_checkpoint_registry: list["Checkpoint"] = []


def register_checkpoint(cp: "Checkpoint") -> None:
    """Register a checkpoint instance for global signal handler access."""
    if cp not in _checkpoint_registry:
        _checkpoint_registry.append(cp)


def force_save_all_checkpoints() -> None:
    """Force save all registered checkpoints (used by signal handler)."""
    for cp in _checkpoint_registry:
        if cp._kotor > 0:
            cp.simpan()
            log.info("Force-saved checkpoint for %s (%d items)", cp.sumber, len(cp.selesai))


class Checkpoint:
    """Catat item yang sudah diproses satu run, supaya run panjang bisa dilanjutkan dengan `--lanjut`.

    Mode:
    - Incremental harian (default): state hanya valid untuk hari yang sama (date.today()).
      Run besok hari -> state otomatis dibersihkan.
      Toleransi default 1 hari untuk cross-midnight runs.
    - Backfill historis (backfill=True): state valid untuk rentang tanggal apa pun.
      Tanggal di state diabaikan, cocok untuk backfill multi-hari/tahun.
    """

    def __init__(self, sumber: str, tanggal: date | None = None, 
                 backfill: bool = False, tolerance_days: int = 1,
                 save_every: int | None = None) -> None:
        self.sumber = sumber
        self.backfill = backfill
        self.tolerance_days = tolerance_days
        self.tanggal = str(tanggal or date.today())
        self.path = DIR_STATE / f"{sumber}.json"
        self.selesai: set[str] = set()
        self._kotor = 0
        # Allow per-instance save threshold override (e.g., 1 for gmaps)
        self._save_every = save_every if save_every is not None else _SIMPAN_SETIAP_DEFAULT
        register_checkpoint(self)

    def __enter__(self) -> "Checkpoint":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._kotor > 0:
            self.simpan()

    def muat(self) -> set[str]:
        """Baca state yang sudah ada; kosongkan bila expired (kecuali backfill).

        Toleransi default 1 hari (cross-midnight OK). Expired jika checkpoint
        lebih lama dari tolerance_days hari dari hari ini.
        """
        if not self.path.exists():
            return self.selesai
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            log.warning("checkpoint %s tidak bisa dibaca (%s); mulai ulang", self.path.name, exc)
            return self.selesai

        # Validasi tanggal dengan toleransi
        if not self.backfill:
            checkpoint_date_str = data.get("tanggal")
            if checkpoint_date_str:
                try:
                    cp_date = datetime.strptime(checkpoint_date_str, "%Y-%m-%d").date()
                    today_date = datetime.strptime(self.tanggal, "%Y-%m-%d").date()
                    delta = (today_date - cp_date).days

                    if delta > self.tolerance_days:
                        log.info(
                            "checkpoint %s expired (%d hari lalu, toleransi %d); mulai ulang",
                            self.path.name, delta, self.tolerance_days
                        )
                        return self.selesai
                    elif delta > 0:
                        log.info(
                            "checkpoint %s dari %d hari lalu (dalam toleransi %d); lanjutkan",
                            self.path.name, delta, self.tolerance_days
                        )
                    else:
                        log.info("checkpoint %s hari ini; lanjutkan", self.path.name)
                except ValueError:
                    log.warning("checkpoint %s format tanggal invalid; mulai ulang", self.path.name)
                    return self.selesai
            else:
                log.info("checkpoint %s tanpa tanggal; lanjutkan", self.path.name)

        selesai = data.get("selesai") or []
        self.selesai = {str(x) for x in selesai}
        log.info("checkpoint %s: %d item sudah diproses%s", self.sumber, len(self.selesai),
                 " (backfill)" if self.backfill else "")
        return self.selesai

    def sudah(self, key: str) -> bool:
        return str(key) in self.selesai

    def tandai(self, key: str) -> None:
        self.selesai.add(str(key))
        self._kotor += 1
        if self._kotor >= self._save_every:
            self.simpan()

    def tandai_kumpulan(self, daftar) -> None:
        for x in daftar:
            self.tandai(x)
        self.simpan()

    def simpan(self) -> None:
        try:
            DIR_STATE.mkdir(parents=True, exist_ok=True)
            isi = {
                "sumber": self.sumber,
                "tanggal": self.tanggal,
                "backfill": self.backfill,
                "jumlah": len(self.selesai),
                "selesai": sorted(self.selesai),
            }
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps(isi, ensure_ascii=False), encoding="utf-8")
            os.replace(tmp, self.path)
            self._kotor = 0
        except OSError as exc:
            log.warning("checkpoint %s gagal ditulis: %s", self.path.name, exc)

    def paksa_simpan(self) -> None:
        """Force save checkpoint regardless of threshold (for interrupt handling)."""
        if self._kotor > 0:
            self.simpan()

    def bersihkan(self) -> None:
        try:
            self.path.unlink(missing_ok=True)
        except OSError as exc:
            log.warning("checkpoint %s gagal dihapus: %s", self.path.name, exc)