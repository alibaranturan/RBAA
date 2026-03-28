"""
launcher.py — Regime Based Asset Allocation
Flask API + file server arka planda, uygulama native pywebview penceresinde açılır.

  • Flask API   → http://localhost:5050  (app.py — subprocess olarak)
  • File server → http://localhost:8765  (HTML/CSS/JS dosyaları)
"""

import sys, os, threading, time, urllib.request, mimetypes, socketserver, subprocess
from http.server import BaseHTTPRequestHandler

PROJECT   = os.path.dirname(os.path.abspath(__file__))
PYTHON    = sys.executable          # launcher ile aynı .venv python'ı
API_PORT  = 5050
FILE_PORT = 8765
APP_URL   = f"http://localhost:{FILE_PORT}/application.html"
PING_URL  = f"http://localhost:{API_PORT}/api/ping"
FILE_PING = f"http://localhost:{FILE_PORT}/application.html"
WIN_W, WIN_H = 1440, 900
STORAGE   = os.path.join(PROJECT, ".webview_storage")


# ── Helpers ────────────────────────────────────────────────────────────────────
def _alive(url):
    try:
        urllib.request.urlopen(url, timeout=2)
        return True
    except Exception:
        return False


def _wait_for(url, label, timeout=40):
    for i in range(timeout):
        time.sleep(1)
        if _alive(url):
            print(f"[launcher] {label} hazır ({i+1}s)")
            return True
    print(f"[launcher] UYARI: {label} {timeout}s içinde yanıt vermedi")
    return False


# ── 1. Flask API — subprocess olarak başlat (thread değil) ────────────────────
_api_proc = None

def _start_api():
    global _api_proc
    if _alive(PING_URL):
        print("[launcher] Flask API zaten çalışıyor —", API_PORT)
        return

    print("[launcher] Flask API başlatılıyor (subprocess) —", API_PORT)
    _api_proc = subprocess.Popen(
        [PYTHON, os.path.join(PROJECT, "app.py")],
        cwd=PROJECT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    _wait_for(PING_URL, "Flask API")

    # Watchdog: API çökerse yeniden başlat
    def _watchdog():
        time.sleep(10)
        while True:
            time.sleep(15)
            if _api_proc and _api_proc.poll() is not None:
                print("[launcher] Flask subprocess öldü, yeniden başlatılıyor…")
                _restart_api()
            elif not _alive(PING_URL):
                print("[launcher] Flask ping yok, yeniden başlatılıyor…")
                _restart_api()
    threading.Thread(target=_watchdog, daemon=True, name="api-watchdog").start()


def _restart_api():
    global _api_proc
    try:
        if _api_proc and _api_proc.poll() is None:
            _api_proc.terminate()
            time.sleep(1)
    except Exception:
        pass
    _api_proc = subprocess.Popen(
        [PYTHON, os.path.join(PROJECT, "app.py")],
        cwd=PROJECT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    _wait_for(PING_URL, "Flask API (restart)")


# ── 2. File server ─────────────────────────────────────────────────────────────
def _file_server_thread():
    SERVE_DIR = PROJECT

    class Handler(BaseHTTPRequestHandler):
        def do_HEAD(self): self._serve(head_only=True)
        def do_GET(self):  self._serve(head_only=False)

        def _serve(self, head_only=False):
            raw   = self.path.split('?')[0].split('#')[0]
            safe  = os.path.normpath('/' + raw.lstrip('/'))
            rel   = safe.lstrip('/')
            fpath = os.path.join(SERVE_DIR, rel) if rel else SERVE_DIR
            if os.path.isdir(fpath):
                for d in ('application.html', 'index.html'):
                    c = os.path.join(fpath, d)
                    if os.path.isfile(c):
                        fpath = c
                        break
                else:
                    self.send_error(403); return
            if not os.path.isfile(fpath):
                self.send_error(404); return
            mime, _ = mimetypes.guess_type(fpath)
            size = os.path.getsize(fpath)
            self.send_response(200)
            self.send_header('Content-Type',   mime or 'application/octet-stream')
            self.send_header('Content-Length', str(size))
            self.send_header('Cache-Control',  'no-cache')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            if not head_only:
                with open(fpath, 'rb') as f:
                    self.wfile.write(f.read())

        def log_message(self, fmt, *args): pass

    class ReuseServer(socketserver.TCPServer):
        allow_reuse_address = True

    try:
        with ReuseServer(('', FILE_PORT), Handler) as httpd:
            httpd.serve_forever()
    except OSError:
        pass
    except Exception as e:
        print(f"[launcher] File server hatası: {e}")


def _start_file_server():
    if _alive(FILE_PING):
        print("[launcher] File server zaten çalışıyor —", FILE_PORT)
        return
    print("[launcher] File server başlatılıyor —", FILE_PORT)
    threading.Thread(target=_file_server_thread, daemon=True, name="file-server").start()
    _wait_for(FILE_PING, "File server")


# ── 3. Titlebar tema senkronizasyonu ──────────────────────────────────────────
def _apply_titlebar(hex_color: str):
    try:
        from AppKit import NSApp, NSColor
        h = hex_color.lstrip('#')
        if len(h) == 3:
            h = ''.join(c * 2 for c in h)
        r, g, b = (int(h[i:i+2], 16) / 255.0 for i in (0, 2, 4))
        color = NSColor.colorWithRed_green_blue_alpha_(r, g, b, 1.0)
        for w in (NSApp.windows() or []):
            w.setTitlebarAppearsTransparent_(True)
            w.setBackgroundColor_(color)
    except Exception as e:
        print(f"[launcher] titlebar: {e}")


def _apply_titlebar_main(hex_color: str):
    try:
        from PyObjCTools.AppHelper import callAfter
        callAfter(_apply_titlebar, hex_color)
    except Exception:
        pass


class _Api:
    def set_theme_color(self, hex_color):
        _apply_titlebar_main(hex_color)


_THEME_SYNC_JS = """
(function(){
    function sync(){
        var bg = getComputedStyle(document.documentElement)
                     .getPropertyValue('--bg').trim() || '#000';
        try{ pywebview.api.set_theme_color(bg); }catch(e){}
    }
    sync();
    new MutationObserver(sync).observe(
        document.documentElement,
        {attributes:true, attributeFilter:['data-theme']}
    );
})();
"""


# ── 4. pywebview penceresi (ana thread) ───────────────────────────────────────
def main():
    _start_api()
    _start_file_server()

    import webview

    api = _Api()
    win = webview.create_window(
        title            = "RBAA — Regime Based Asset Allocation",
        url              = APP_URL,
        width            = WIN_W,
        height           = WIN_H,
        resizable        = True,
        text_select      = True,
        background_color = "#000000",
        js_api           = api,
    )

    def _on_loaded():
        try:
            win.evaluate_js(_THEME_SYNC_JS)
        except Exception:
            pass
        _apply_titlebar_main("#000000")

    win.events.loaded += _on_loaded

    try:
        webview.start(private_mode=False, storage_path=STORAGE)
    finally:
        # Pencere kapanınca API subprocess'i de temizle
        if _api_proc and _api_proc.poll() is None:
            _api_proc.terminate()


if __name__ == "__main__":
    main()
