#!/bin/bash
# Flask API sunucusunu başlatır — hangi dizinden çalıştırılırsa çalıştırılsın

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP="$SCRIPT_DIR/app.py"

# Projenin kendi .venv'ini kullan
VENV="$SCRIPT_DIR/.venv/bin/python"

# Fallback: PyCharmMiscProject veya sistem python3
if [ ! -f "$VENV" ]; then
  VENV="$HOME/PyCharmMiscProject/.venv/bin/python"
fi
if [ ! -f "$VENV" ]; then
  VENV="python3"
fi

# Varsa çalışan eski process'i durdur
lsof -ti :5050 | xargs kill -9 2>/dev/null
sleep 1

echo "[start_server] Başlatılıyor: $VENV $APP"
echo "[start_server] Log: /tmp/flask_2.log"

nohup "$VENV" "$APP" > /tmp/flask_2.log 2>&1 &
PID=$!
echo "[start_server] PID=$PID"

sleep 4
if curl -s --max-time 3 "http://127.0.0.1:5050/api/ping" > /dev/null; then
  echo "[start_server] ✓ API çevrimiçi → http://localhost:5050"
else
  echo "[start_server] ✗ Başlatılamadı. Log:"
  cat /tmp/flask_2.log
fi
