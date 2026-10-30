"""Optional real-browser smoke test for the local Cytoscape.js viewer.

Requires a Chrome/Chromium binary and network access for the pinned CDN
script. Runs against the synthetic demo, not against live biological data.
"""
from __future__ import annotations

import html
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
from threading import Thread


EXPLORER = Path(__file__).resolve().parent.parent


def main() -> None:
    chrome = shutil.which("google-chrome") or shutil.which("chromium")
    if not chrome:
        raise RuntimeError("Chrome/Chromium is required for the browser smoke test")
    with tempfile.TemporaryDirectory(prefix="eic-explorer-smoke-") as directory:
        root = Path(directory)
        shutil.copytree(EXPLORER, root / "explorer")
        index = root / "explorer" / "index.html"
        page = index.read_text(encoding="utf-8")
        marker = "</body>"
        assert marker in page, "HTML body is missing"
        page = page.replace(
            marker,
            "<script>"
            "window.addEventListener('load',function(){"
            "setTimeout(function(){document.getElementById('demo').click()},500)"
            "})"
            "</script>" + marker,
        )
        index.write_text(page, encoding="utf-8")

        class Handler(SimpleHTTPRequestHandler):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, directory=directory, **kwargs)

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            url = f"http://127.0.0.1:{server.server_port}/explorer/"
            result = subprocess.run(
                [
                    chrome, "--headless", "--no-sandbox", "--disable-dev-shm-usage",
                    "--disable-gpu", "--disable-extensions", "--no-first-run",
                    f"--user-data-dir={root / 'chrome-profile'}",
                    "--virtual-time-budget=16000", "--dump-dom", url,
                ],
                capture_output=True, text=True, timeout=90, check=False,
            )
        finally:
            server.shutdown()
            server.server_close()
        if result.returncode:
            raise AssertionError(
                f"Headless Chrome exited {result.returncode}: {result.stderr[-1500:]}"
            )
        output = html.unescape(result.stdout)
        # The current demo is deliberately synthetic: 5 nodes, 4 edges.
        count = re.search(r'id="graph-count"[^>]*>([^<]*)</span>', output)
        dataset = re.search(r'id="dataset-name"[^>]*>([^<]*)</span>', output)
        shown = count.group(1) if count else "not found"
        source = dataset.group(1) if dataset else "not found"
        if "Узлов: 5" not in shown or "связей: 4" not in shown or "синтетические" not in source:
            load_status = re.search(r'id="load-status"[^>]*>([^<]*)</span>', output)
            detail = load_status.group(1) if load_status else "not found"
            raise AssertionError(
                f"Demo graph not rendered. count={shown!r}, source={source!r}, "
                f"status={detail!r}, Chrome={result.stderr[-900:]}"
            )
        print("Headless Chrome smoke passed:", source, shown)


if __name__ == "__main__":
    main()
