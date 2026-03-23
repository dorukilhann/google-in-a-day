from __future__ import annotations

import json
import shutil
import threading
import time
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.request import urlopen

from google_in_a_day.engine import CrawlerEngine
from google_in_a_day.web import make_handler


PAGES = {
    "/": """
        <html>
          <head><title>Alpha Root</title></head>
          <body>
            alpha alpha beta
            <a href="/docs">docs</a>
          </body>
        </html>
    """,
    "/docs": """
        <html>
          <head><title>Beta Docs</title></head>
          <body>
            beta beta beta gamma
            <a href="/deep">deep</a>
          </body>
        </html>
    """,
    "/deep": """
        <html>
          <head><title>Gamma Deep</title></head>
          <body>
            gamma alpha
          </body>
        </html>
    """,
}


class PageHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        body = PAGES.get(self.path)
        if body is None:
            self.send_response(404)
            self.end_headers()
            return
        encoded = body.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        if self.path == "/docs":
            time.sleep(0.2)
        self.wfile.write(encoded)

    def log_message(self, format: str, *args) -> None:
        return


class EngineTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), PageHandler)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def setUp(self) -> None:
        temp_root = Path(__file__).resolve().parents[1] / "data" / "testdbs"
        temp_root.mkdir(parents=True, exist_ok=True)
        self.db_path = str(temp_root / f"{uuid.uuid4().hex}.db")
        self.storage_dir = str(temp_root / f"{uuid.uuid4().hex}_storage")
        self.engine = CrawlerEngine(db_path=self.db_path, storage_dir=self.storage_dir)

    def tearDown(self) -> None:
        self.engine.store.close()
        db_path = Path(self.db_path)
        wal_path = db_path.with_suffix(".db-wal")
        shm_path = db_path.with_suffix(".db-shm")
        for path in [db_path, wal_path, shm_path]:
            if path.exists():
                path.unlink()
        storage_path = Path(self.storage_dir)
        if storage_path.exists():
            shutil.rmtree(storage_path)

    def wait_for_job(self, job_id: str, timeout: float = 5.0) -> dict:
        deadline = time.time() + timeout
        while time.time() < deadline:
            snapshot = self.engine.get_job_snapshot(job_id)
            if snapshot and snapshot["status"] == "completed":
                return snapshot
            time.sleep(0.05)
        self.fail(f"job {job_id} did not complete")

    def test_crawl_depth_and_search(self) -> None:
        origin = f"http://127.0.0.1:{self.port}/"
        job = self.engine.start_job(origin=origin, max_depth=1, rate_limit=20, queue_capacity=10, worker_count=2)
        snapshot = self.wait_for_job(job["job_id"])
        self.assertEqual(snapshot["pages_indexed"], 2)

        results = self.engine.search("beta")
        triples = {(item.relevant_url, item.origin_url, item.depth) for item in results}
        self.assertIn((origin, origin, 0), triples)
        self.assertIn((f"http://127.0.0.1:{self.port}/docs", origin, 1), triples)
        self.assertNotIn((f"http://127.0.0.1:{self.port}/deep", origin, 2), triples)

    def test_persistence_reloads_index(self) -> None:
        origin = f"http://127.0.0.1:{self.port}/"
        job = self.engine.start_job(origin=origin, max_depth=2, rate_limit=20, queue_capacity=10, worker_count=2)
        self.wait_for_job(job["job_id"])
        self.engine.store.close()

        reloaded = CrawlerEngine(db_path=self.db_path, storage_dir=self.storage_dir)
        try:
            results = reloaded.search("gamma")
            serialized = json.dumps(
                [
                    {
                        "url": result.relevant_url,
                        "origin": result.origin_url,
                        "depth": result.depth,
                    }
                    for result in results
                ]
            )
            self.assertIn("/docs", serialized)
            self.assertIn("/deep", serialized)
        finally:
            reloaded.store.close()

    def test_raw_storage_and_relevance_search(self) -> None:
        origin = f"http://127.0.0.1:{self.port}/"
        job = self.engine.start_job(origin=origin, max_depth=1, rate_limit=20, queue_capacity=10, worker_count=2)
        self.wait_for_job(job["job_id"])

        storage_file = Path(self.storage_dir) / "b.data"
        self.assertTrue(storage_file.exists())
        lines = storage_file.read_text(encoding="utf-8").splitlines()
        self.assertIn(f"beta {origin} {origin} 0 1", lines)
        self.assertIn(f"beta http://127.0.0.1:{self.port}/docs {origin} 1 3", lines)

        results = self.engine.search("beta")
        self.assertEqual(results[0].relevant_url, f"http://127.0.0.1:{self.port}/docs")
        self.assertEqual(results[0].score, (3 * 10) + 1000 - (1 * 5))

    def test_search_route_supports_expected_search_format(self) -> None:
        origin = f"http://127.0.0.1:{self.port}/"
        job = self.engine.start_job(origin=origin, max_depth=1, rate_limit=20, queue_capacity=10, worker_count=2)
        self.wait_for_job(job["job_id"])

        api_server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(self.engine))
        api_port = api_server.server_address[1]
        api_thread = threading.Thread(target=api_server.serve_forever, daemon=True)
        api_thread.start()
        try:
            with urlopen(f"http://127.0.0.1:{api_port}/search?query=beta&sortBy=relevance", timeout=2) as response:
                payload = json.loads(response.read().decode("utf-8"))
            self.assertEqual(payload["results"][0]["url"], f"http://127.0.0.1:{self.port}/docs")
            self.assertEqual(payload["results"][0]["relevance_score"], (3 * 10) + 1000 - (1 * 5))
        finally:
            api_server.shutdown()
            api_server.server_close()
            api_thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
