"""Minimal HTTP file server for Optimization PyCharm 2.0.

Serves the project folder (application.html, dashboard.html, log.html, etc.)
with CORS headers so the browser can reach the Flask API on port 5050.
"""
import os, mimetypes, socketserver
from http.server import BaseHTTPRequestHandler

SERVE_DIR = os.path.realpath(os.path.dirname(os.path.abspath(__file__)))
PORT = int(os.environ.get('PORT', '8765'))


class Handler(BaseHTTPRequestHandler):
    def do_HEAD(self):
        self._serve(head_only=True)

    def do_GET(self):
        self._serve(head_only=False)

    def _serve(self, head_only=False):
        raw = self.path.split('?')[0].split('#')[0]
        safe = os.path.normpath('/' + raw.lstrip('/'))
        rel  = safe.lstrip('/')
        fpath = os.path.join(SERVE_DIR, rel) if rel else SERVE_DIR

        # Directory → look for application.html, then index.html
        if os.path.isdir(fpath):
            for default in ('application.html', 'index.html'):
                candidate = os.path.join(fpath, default)
                if os.path.isfile(candidate):
                    fpath = candidate
                    break
            else:
                self.send_error(403, 'Directory listing not allowed')
                return

        if not os.path.isfile(fpath):
            self.send_error(404, 'File not found')
            return

        mime, _ = mimetypes.guess_type(fpath)
        mime = mime or 'application/octet-stream'
        size = os.path.getsize(fpath)

        self.send_response(200)
        self.send_header('Content-Type',   mime)
        self.send_header('Content-Length', str(size))
        self.send_header('Cache-Control',  'no-cache')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

        if not head_only:
            with open(fpath, 'rb') as f:
                self.wfile.write(f.read())

    def log_message(self, fmt, *args):
        print(fmt % args, flush=True)


class ReusableTCPServer(socketserver.TCPServer):
    allow_reuse_address = True


print(f'Serving {SERVE_DIR} on http://localhost:{PORT}', flush=True)
with ReusableTCPServer(('', PORT), Handler) as httpd:
    httpd.serve_forever()
