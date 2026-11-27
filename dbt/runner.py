import hmac
import os
import subprocess
import threading
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException


PROJECT_DIR = Path(__file__).resolve().parent
RUN_LOCK = threading.Lock()
app = FastAPI(title="DataFlow360 dbt runner")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/run")
def run_build(x_pipeline_token: str | None = Header(default=None)):
    expected = os.getenv("PIPELINE_INTERNAL_TOKEN", "")
    if not expected or not x_pipeline_token or not hmac.compare_digest(
        expected, x_pipeline_token
    ):
        raise HTTPException(status_code=401, detail="Invalid internal pipeline token")

    if not RUN_LOCK.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="A dbt build is already running")
    try:
        result = subprocess.run(
            [
                "dbt",
                "build",
                "--project-dir",
                str(PROJECT_DIR),
                "--profiles-dir",
                "/root/.dbt",
            ],
            cwd=PROJECT_DIR,
            capture_output=True,
            text=True,
            timeout=1800,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise HTTPException(status_code=504, detail="dbt build exceeded 30 minutes") from exc
    except OSError as exc:
        raise HTTPException(status_code=503, detail=f"Could not start dbt: {exc}") from exc
    finally:
        RUN_LOCK.release()

    output = (result.stdout + result.stderr)[-8000:]
    if result.returncode:
        raise HTTPException(
            status_code=500,
            detail=f"dbt build failed with exit code {result.returncode}: {output}",
        )
    return {"state": "success", "output": output}
