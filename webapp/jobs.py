"""Background upload job management."""

import logging
import threading
import uuid
from datetime import datetime

from loader import config
from loader.main import load_all, run_migrations
from webapp.db import connect

jobs: dict = {}


class _LogCapture(logging.Handler):
    def __init__(self, job):
        super().__init__()
        self.job = job

    def emit(self, record):
        msg = self.format(record)
        self.job["log"].append(msg)
        if self.job.get("batch_id"):
            try:
                conn = connect()
                try:
                    with conn.cursor() as cur:
                        cur.execute("""
                            UPDATE catalog.import_batch
                            SET log_text = COALESCE(log_text, '') || %s || E'\\n'
                            WHERE id = %s
                        """, (msg, self.job["batch_id"]))
                    conn.commit()
                finally:
                    conn.close()
            except Exception:
                pass


def _run_job(job_id: str):
    job = jobs[job_id]
    handler = _LogCapture(job)
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s: %(message)s", "%H:%M:%S")
    )

    targets = [
        logging.getLogger(n)
        for n in ("loader.main", "loader.discover", "loader.ingest", "loader.catalog")
    ]
    for lg in targets:
        lg.addHandler(handler)

    try:
        conn = connect()
        try:
            run_migrations(conn)
            load_all(conn)
            job["status"] = "completed"
        except Exception as exc:
            job["status"] = "failed"
            job["error"] = str(exc)
        finally:
            conn.close()
    finally:
        for lg in targets:
            lg.removeHandler(handler)
        job["finished"] = datetime.now().isoformat()


def start_job(filename: str) -> str:
    """Create and launch a background import job. Returns job_id."""
    job_id = uuid.uuid4().hex[:8]
    jobs[job_id] = {
        "id": job_id,
        "filename": filename,
        "status": "running",
        "started": datetime.now().isoformat(),
        "finished": None,
        "log": [],
        "error": None,
    }
    threading.Thread(target=_run_job, args=(job_id,), daemon=True).start()
    return job_id
