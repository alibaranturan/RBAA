"""
Claude Preview launcher for the Regime Based Asset Allocation Flask API.
Starts Flask app.py directly using the project's .venv, then waits for it to be reachable.
"""
import subprocess, urllib.request, time, sys, os

TARGET  = 'http://localhost:5050/api/ping'
PROJECT = os.path.dirname(os.path.abspath(__file__))
VENV_PY = os.path.join(PROJECT, '.venv', 'bin', 'python')
APP_PY  = os.path.join(PROJECT, 'app.py')


def api_alive():
    try:
        urllib.request.urlopen(TARGET, timeout=2)
        return True
    except Exception:
        return False


# ── 1. Check if API is already running ───────────────────────────────────────
if api_alive():
    print('[run_api] API already running on 5050.', flush=True)
else:
    # ── 2. Launch Flask via venv python ──────────────────────────────────────
    try:
        subprocess.Popen(
            [VENV_PY, APP_PY],
            cwd=PROJECT,
            stdout=open('/tmp/flask_2.log', 'w'),
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        print('[run_api] Launched Flask via .venv python', flush=True)
    except Exception as e:
        print(f'[run_api] Launch failed: {e}', flush=True)
        sys.exit(1)

    # ── 3. Wait for API to come up (up to 30 s) ──────────────────────────────
    for i in range(30):
        if api_alive():
            print(f'[run_api] API online after {i+1}s', flush=True)
            break
        time.sleep(1)
    else:
        print('[run_api] WARNING: API did not respond on 5050.', flush=True)

# ── 4. Keep process alive for preview_start ───────────────────────────────────
print('[run_api] Holding (press Ctrl+C to stop).', flush=True)
try:
    while True:
        time.sleep(30)
        if not api_alive():
            print('[run_api] API went offline!', flush=True)
except KeyboardInterrupt:
    sys.exit(0)
