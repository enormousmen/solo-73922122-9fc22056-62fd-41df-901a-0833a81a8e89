#!/usr/bin/env bash
# 本机无 Docker 时的开发/演示启动脚本（容器环境请用 docker compose up）。
# 起三个进程：旧站 :8011、控制台 A :8010、控制台 B :8012；
# 两个控制台共享同一 SQLite 与上传目录（data/）。
# 用法: ./run-local.sh
set -euo pipefail
cd "$(dirname "$0")"

VENV="${VENV:-.venv}"
PY="$PWD/$VENV/bin/python"
LEGACY_PORT="${LEGACY_PORT:-8011}"
REPORTER_A_PORT="${REPORTER_A_PORT:-8010}"
REPORTER_B_PORT="${REPORTER_B_PORT:-8012}"
export REPORTER_DB="${REPORTER_DB:-$PWD/data/reporter.db}"
export UPLOAD_DIR="${UPLOAD_DIR:-$PWD/data/uploads}"
export SCREENSHOT_DIR="${SCREENSHOT_DIR:-$PWD/data/screenshots}"
export LEGACY_DB="${LEGACY_DB:-$PWD/data/legacy.db}"
export LEGACY_URL="http://127.0.0.1:${LEGACY_PORT}"
export HEADLESS="${HEADLESS:-1}"
export BROWSER_CHANNEL="${BROWSER_CHANNEL:-chromium}"   # 本机只有完整 Chromium
export SLOW_MO_MS="${SLOW_MO_MS:-150}"

# 本机 Chromium 依赖库解压在用户目录时，需要补 LD_LIBRARY_PATH
if [ -d "$HOME/.local/chromelibs" ]; then
  export LD_LIBRARY_PATH="$(find "$HOME/.local/chromelibs/usr/lib" "$HOME/.local/chromelibs/lib" \
      -type d 2>/dev/null | tr '\n' ':')${LD_LIBRARY_PATH:-}"
fi

mkdir -p data/uploads/incoming data/screenshots
"$PY" -m uvicorn legacy.app:app --host 127.0.0.1 --port "$LEGACY_PORT" \
  > data/legacy.log 2>&1 &
LEGACY_PID=$!
echo "legacy station pid=$LEGACY_PID -> http://127.0.0.1:${LEGACY_PORT}"

CONSOLE_NAME=A PEER_CONSOLE_URL="http://127.0.0.1:${REPORTER_B_PORT}" \
  "$PY" -m uvicorn reporter.app:app --host 127.0.0.1 --port "$REPORTER_A_PORT" \
  > data/reporter-a.log 2>&1 &
REPORTER_A_PID=$!
echo "reporter console A pid=$REPORTER_A_PID -> http://127.0.0.1:${REPORTER_A_PORT}"

CONSOLE_NAME=B PEER_CONSOLE_URL="http://127.0.0.1:${REPORTER_A_PORT}" \
  "$PY" -m uvicorn reporter.app:app --host 127.0.0.1 --port "$REPORTER_B_PORT" \
  > data/reporter-b.log 2>&1 &
REPORTER_B_PID=$!
echo "reporter console B pid=$REPORTER_B_PID -> http://127.0.0.1:${REPORTER_B_PORT}"

echo "$LEGACY_PID" > data/legacy.pid
echo "$REPORTER_A_PID" > data/reporter-a.pid
echo "$REPORTER_B_PID" > data/reporter-b.pid
trap 'kill $LEGACY_PID $REPORTER_A_PID $REPORTER_B_PID 2>/dev/null || true' INT TERM
wait
