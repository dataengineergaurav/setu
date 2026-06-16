"""
setu Demo — Webhook Display Server

Receives POST /events from the engine, appends them to an in-memory
list, prints each event to stdout, and serves a real-time HTML page
at GET / showing all received events.

Usage:
    python3 server.py          # listens on 0.0.0.0:8080
"""

import http.server
import json
import os
import threading
from html import escape
from urllib.parse import urlparse

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8080"))

events: list[dict] = []
lock = threading.Lock()

PAGE_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="3">
<title>setu Demo</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
          background: #0d1117; color: #c9d1d9; padding: 2rem; }}
  h1 {{ color: #58a6ff; margin-bottom: 0.5rem; }}
  .sub {{ color: #8b949e; margin-bottom: 2rem; }}
  .event {{ background: #161b22; border: 1px solid #30363d; border-radius: 6px;
            padding: 1rem; margin-bottom: 0.75rem; font-size: 0.9rem; }}
  .event .meta {{ color: #8b949e; margin-bottom: 0.5rem; }}
  .event .meta strong {{ color: #c9d1d9; }}
  .event .table {{ color: #79c0ff; }}
  .event .op-Insert {{ color: #3fb950; }}
  .event .op-Update {{ color: #d29922; }}
  .event .op-Delete {{ color: #f85149; }}
  pre {{ background: #0d1117; border: 1px solid #30363d; border-radius: 4px;
         padding: 0.75rem; overflow-x: auto; font-size: 0.8rem; }}
  code {{ color: #ffa657; }}
  .empty {{ color: #484f58; text-align: center; margin-top: 4rem; }}
  .badge {{ display: inline-block; background: #1f6feb; color: #fff; border-radius: 12px;
            padding: 0.15rem 0.6rem; font-size: 0.75rem; font-weight: 600; }}
</style>
</head>
<body>
<h1>&#9889; setu Demo</h1>
<p class="sub">Received <span class="badge">{count}</span> events</p>
{events_html}
</body>
</html>
"""

EVENT_CARD = """\
<div class="event">
  <div class="meta">
    <strong class="table">{table}</strong>
    <span class="op-{op}">{op}</span>
    &middot;
    <code>{dest}</code>
  </div>
  <pre>{payload}</pre>
</div>
"""


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path != "/":
            self.send_response(404)
            self.end_headers()
            return

        with lock:
            count = len(events)
            cards = reversed(events[-100:])  # show latest 100
            events_html = "".join(
                EVENT_CARD.format(
                    table=escape(e.get("table", "?")),
                    op=escape(e.get("op", "?")),
                    dest=escape(e.get("destination", "?")),
                    payload=escape(json.dumps(e.get("payload", {}), indent=2)),
                )
                for e in cards
            ) or '<p class="empty">No events yet. Run the seed script to generate some.</p>'

        body = PAGE_HTML.format(count=count, events_html=events_html).encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path != "/events":
            self.send_response(404)
            self.end_headers()
            return

        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = {"raw": raw.decode("utf-8", errors="replace")}

        event_type = self.headers.get("X-Event-Type", "unknown")
        table = payload.get("table", "?")
        op = payload.get("op", "?")

        entry = {
            "table": table,
            "op": op,
            "destination": event_type,
            "payload": payload,
        }

        with lock:
            events.append(entry)

        print(f"[EVENT] table={table} op={op} type={event_type}")
        print(json.dumps(payload, indent=2))
        print("-" * 50)

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"ok"}')

    def log_message(self, fmt, *args):
        print(f"[HTTP] {fmt % args}")


if __name__ == "__main__":
    server = http.server.HTTPServer((HOST, PORT), Handler)
    print(f"Webhook Display Server listening on http://{HOST}:{PORT}")
    print(f"  POST /events — engine delivers events here")
    print(f"  GET  /       — HTML dashboard showing all events")
    server.serve_forever()
