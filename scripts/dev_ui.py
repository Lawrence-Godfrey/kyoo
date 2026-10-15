# Serve the working-copy UI against the running daemon: python3 scripts/dev_ui.py src/kyoo/ui/index.html, then open http://127.0.0.1:7799/?token=$(cat ~/.local/share/kyoo/token)
import http.server, urllib.request, sys
UI = sys.argv[1]; UP = "http://127.0.0.1:7777"
class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_GET(self):
        if self.path.split("?")[0] == "/":
            b = open(UI, "rb").read(); self.send_response(200); self.send_header("Content-Type", "text/html"); self.end_headers(); self.wfile.write(b); return
        r = urllib.request.Request(UP + self.path, headers={"Authorization": self.headers.get("Authorization", "")})
        try:
            with urllib.request.urlopen(r) as u: b = u.read(); self.send_response(u.status); self.send_header("Content-Type", u.headers.get("Content-Type", "")); self.end_headers(); self.wfile.write(b)
        except urllib.error.HTTPError as e: self.send_response(e.code); self.end_headers()
http.server.ThreadingHTTPServer(("127.0.0.1", 7799), H).serve_forever()
