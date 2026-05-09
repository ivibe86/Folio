#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKEND_PORT="${BACKEND_PORT:-8000}"
PYTHON_BIN="${PYTHON_BIN:-}"
DEPENDENCY_COOLDOWN_DAYS="${FOLIO_DEPENDENCY_COOLDOWN_DAYS:-7}"
PIP_VERSION="${FOLIO_PIP_VERSION:-26.0.1}"

if [[ -z "${PYTHON_BIN}" ]]; then
  if [[ -x "${ROOT_DIR}/.venv/bin/python" ]]; then
    PYTHON_BIN="${ROOT_DIR}/.venv/bin/python"
  else
    PYTHON_BIN="python3"
  fi
fi

export DEMO_MODE="${DEMO_MODE:-1}"
export DB_FILE="${DB_FILE:-${ROOT_DIR}/backend/Folio-demo.db}"
export Folio_API_KEY="${Folio_API_KEY:-folio-demo-key}"
export VITE_API_KEY="${VITE_API_KEY:-${Folio_API_KEY}}"
export PORT="${PORT:-3000}"
export HOST="${HOST:-0.0.0.0}"
export BACKEND_URL="${BACKEND_URL:-http://127.0.0.1:${BACKEND_PORT}}"
export LLM_PROVIDER="${LLM_PROVIDER:-none}"
export ENABLE_LOCAL_ENRICHMENT="${ENABLE_LOCAL_ENRICHMENT:-false}"
export ENABLE_LLM_CATEGORIZATION="${ENABLE_LLM_CATEGORIZATION:-false}"

if [[ -z "${TOKEN_ENCRYPTION_KEY:-}" ]]; then
  export TOKEN_ENCRYPTION_KEY="$("${PYTHON_BIN}" - <<'PY'
try:
    from cryptography.fernet import Fernet
    print(Fernet.generate_key().decode())
except Exception:
    import base64
    import secrets
    print(base64.urlsafe_b64encode(secrets.token_bytes(32)).decode())
PY
)"
fi

if ! "${PYTHON_BIN}" -c "import fastapi, uvicorn" >/dev/null 2>&1; then
  CUTOFF="$(DEPENDENCY_COOLDOWN_DAYS="${DEPENDENCY_COOLDOWN_DAYS}" "${PYTHON_BIN}" - <<'PY'
import os
from datetime import datetime, timezone, timedelta
days = int(os.environ["DEPENDENCY_COOLDOWN_DAYS"])
print((datetime.now(timezone.utc) - timedelta(days=days)).replace(microsecond=0).isoformat().replace("+00:00", "Z"))
PY
)"
  echo "Missing backend Python dependencies for demo mode."
  echo "Install them with:"
  echo "  ${PYTHON_BIN} -m pip install --upgrade pip==${PIP_VERSION}"
  echo "  ${PYTHON_BIN} -m pip install --require-hashes --uploaded-prior-to=${CUTOFF} -r ${ROOT_DIR}/backend/requirements.lock"
  exit 1
fi

"${PYTHON_BIN}" "${ROOT_DIR}/backend/create_demo_db.py" --output "${DB_FILE}" --force

if [[ ! -d "${ROOT_DIR}/frontend/build" ]]; then
  (
    cd "${ROOT_DIR}/frontend"
    DOCKER=true VITE_API_KEY="${VITE_API_KEY}" npm run build
  )
fi

cleanup() {
  if [[ -n "${BACKEND_PID:-}" ]]; then kill "${BACKEND_PID}" 2>/dev/null || true; fi
  if [[ -n "${FRONTEND_PID:-}" ]]; then kill "${FRONTEND_PID}" 2>/dev/null || true; fi
}
trap cleanup EXIT INT TERM

(
  cd "${ROOT_DIR}/backend"
  "${PYTHON_BIN}" -m uvicorn main:app --host 127.0.0.1 --port "${BACKEND_PORT}"
) &
BACKEND_PID=$!

(
  cd "${ROOT_DIR}/frontend"
  node build
) &
FRONTEND_PID=$!

while kill -0 "${BACKEND_PID}" 2>/dev/null && kill -0 "${FRONTEND_PID}" 2>/dev/null; do
  sleep 1
done

if ! kill -0 "${BACKEND_PID}" 2>/dev/null; then
  wait "${BACKEND_PID}"
else
  wait "${FRONTEND_PID}"
fi
