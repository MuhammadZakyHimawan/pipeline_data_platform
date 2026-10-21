#!/bin/bash
# run_pipeline.sh - Pipeline wrapper dengan auto-retry dan alerting
# Usage: ./run_pipeline.sh [pipeline_args...]
# Example: ./run_pipeline.sh --harian --bronze --lanjut
#          ./run_pipeline.sh --bulanan --bronze --lanjut
#          ./run_pipeline.sh --tahunan

set -euo pipefail

# ============================================================
# KONFIGURASI (bisa di-override via environment variables)
# ============================================================
MAX_RETRIES="${MAX_RETRIES:-3}"
RETRY_DELAY="${RETRY_DELAY:-30}"  # detik
PYTHON_CMD="${PYTHON_CMD:-python}"
MAIN_SCRIPT="main.py"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="${PROJECT_ROOT}/logs"
ALERT_SCRIPT="${PROJECT_ROOT}/src/shared/pipeline_alert.py"

# Pastikan log directory exists
mkdir -p "${LOG_DIR}"

# Load environment variables dari config/.env jika ada
if [[ -f "${PROJECT_ROOT}/config/.env" ]]; then
    # shellcheck source=/dev/null
    source "${PROJECT_ROOT}/config/.env"
fi

# ============================================================
# FUNGSI HELPER
# ============================================================
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

alert_failure() {
    local exit_code=$1
    local message=$2
    local schedule=${3:-"unknown"}
    
    log "🚨 Mengirim alert kegagalan..."
    "${PYTHON_CMD}" "${ALERT_SCRIPT}" \
        --message "${message}" \
        --exit-code "${exit_code}" \
        --source "${schedule}" \
        || log "⚠️ Alert script gagal dijalankan (lanjutkan exit)"
}

# ============================================================
# PARSING ARGUMEN
# ============================================================
# Default args (bisa di-override saat pemanggilan)
DEFAULT_ARGS=("--harian" "--bronze" "--lanjut")

# Jika tidak ada argumen, gunakan default
if [[ $# -eq 0 ]]; then
    PIPELINE_ARGS=("${DEFAULT_ARGS[@]}")
    SCHEDULE="harian"
else
    PIPELINE_ARGS=("$@")
    # Detek schedule dari argumen untuk alert context
    SCHEDULE="custom"
    for arg in "$@"; do
        case "${arg}" in
            --harian) SCHEDULE="harian" ;;
            --bulanan) SCHEDULE="bulanan" ;;
            --tahunan) SCHEDULE="tahunan" ;;
            --schedule) 
                # Next argument is the schedule value
                shift
                SCHEDULE="${1:-custom}"
                ;;
        esac
    done
fi

log "============================================"
log "🚀 PIPELINE WRAPPER STARTED"
log "============================================"
log "Schedule: ${SCHEDULE}"
log "Args: ${PIPELINE_ARGS[*]}"
log "Max Retries: ${MAX_RETRIES}"
log "Retry Delay: ${RETRY_DELAY}s"
log "Project Root: ${PROJECT_ROOT}"
log "============================================"

# ============================================================
# MAIN RETRY LOOP
# ============================================================
for attempt in $(seq 1 "${MAX_RETRIES}"); do
    log "📋 Attempt ${attempt}/${MAX_RETRIES}: ${PYTHON_CMD} ${MAIN_SCRIPT} ${PIPELINE_ARGS[*]}"
    
    # Jalankan pipeline
    # Note: Kita tidak pakai 'set -e' di sini agar bisa tangkap exit code
    "${PYTHON_CMD}" "${MAIN_SCRIPT}" "${PIPELINE_ARGS[@]}"
    EXIT_CODE=$?
    
    if [[ ${EXIT_CODE} -eq 0 ]]; then
        log "✅ Pipeline BERHASIL (exit code: 0)"
        log "============================================"
        exit 0
    fi
    
    log "❌ Pipeline GAGAL (exit code: ${EXIT_CODE})"
    
    if [[ ${attempt} -lt ${MAX_RETRIES} ]]; then
        log "⏳ Menunggu ${RETRY_DELAY} detik sebelum retry..."
        sleep "${RETRY_DELAY}"
    fi
done

# ============================================================
# SEMUA RETRY GAGAL - KIRIM ALERT
# ============================================================
log "💥 SEMUA RETRY GAGAL (${MAX_RETRIES} percobaan)"
alert_failure "${EXIT_CODE}" \
    "Pipeline '${SCHEDULE}' GAGAL setelah ${MAX_RETRIES} kali retry. Butuh pengecekan manual!" \
    "${SCHEDULE}"

log "============================================"
log "🛑 PIPELINE WRAPPER EXIT WITH ERROR"
log "============================================"
exit 1