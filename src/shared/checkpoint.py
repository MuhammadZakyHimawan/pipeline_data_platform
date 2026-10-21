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
_SIMPAN_SETIAP = 10


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
                 backfill: bool = False, tolerance_days: int = 1) -> None:
        self.sumber = sumber
        self.backfill = backfill
        self.tolerance_days = tolerance_days
        self.tanggal = str(tanggal or date.today())
        self.path = DIR_STATE / f"{sumber}.json"
        self.selesai: set[str] = set()
        self._kotor = 0

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
        if self._kotor >= _SIMPAN_SETIAP:
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

    def bersihkan(self) -> None:
        try:
            self.path.unlink(missing_ok=True)
        except OSError as exc:
            log.warning("checkpoint %s gagal dihapus: %s", self.path.name, exc)