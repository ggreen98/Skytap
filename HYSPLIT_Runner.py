from __future__ import annotations
import os
import subprocess
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import yaml
import time
from datetime import datetime

CONFIG_PATH = Path("Config.yaml")

def load_config():
    with CONFIG_PATH.open() as f:
        return yaml.safe_load(f)
    
cfg = load_config() # load config.yaml

TEMP_ROOT = Path(cfg["temp_HYSPLIT_config_dir"])
PROJECT_ROOT = Path(__file__).resolve().parent  # folder containing this runner script
HYSPLIT_EXE = (PROJECT_ROOT / cfg["hysplit"]["exec_path"]).resolve()
TRAJ_ROOT = (PROJECT_ROOT / cfg["hysplit"]["traj_root"]).resolve()
TRAJ_ROOT.mkdir(parents=True, exist_ok=True)


if not HYSPLIT_EXE.exists():
    raise FileNotFoundError(f"HYSPLIT executable not found: {HYSPLIT_EXE}")

def run_hysplit_in_dir(run_dir: Path, timeout_s: int = 300) -> tuple[str, int, str]:
    start_ts = datetime.now().strftime("%H:%M:%S")
    print(f"[START {start_ts}] {run_dir.name}", flush=True)

    ctl = run_dir / "CONTROL"
    if not ctl.exists():
        return run_dir.name, 999, "Missing CONTROL"

    stdout_path = run_dir / "run_stdout.txt"
    stderr_path = run_dir / "run_stderr.txt"

    for fname in ("tdump", "MESSAGE"):
        p = run_dir / fname
        if p.exists():
            p.unlink()

    env = os.environ.copy()

    with stdout_path.open("w") as out, stderr_path.open("w") as err:
        try:
            proc = subprocess.run(
                [str(HYSPLIT_EXE)],
                cwd=str(run_dir),
                stdout=out,
                stderr=err,
                env=env,
                timeout=timeout_s,
                check=False,
            )
            rc = proc.returncode
        except subprocess.TimeoutExpired:
            print(f"[TIMEOUT] {run_dir.name}", flush=True)
            return run_dir.name, 124, f"Timeout after {timeout_s}s"
        except FileNotFoundError as e:
            return run_dir.name, 127, f"Executable not found: {e}"

    end_ts = datetime.now().strftime("%H:%M:%S")
    print(f"[END   {end_ts}] {run_dir.name} (rc={rc})", flush=True)

    tdump = run_dir / "tdump"
    if rc == 0 and tdump.exists() and tdump.stat().st_size > 0:
        return run_dir, run_dir.name, 0, "OK"

    debug = ""
    if stderr_path.exists():
        debug = stderr_path.read_text(errors="ignore")[-2000:]
    msg = run_dir / "MESSAGE"
    if (not debug) and msg.exists():
        debug = msg.read_text(errors="ignore")[-2000:]

    return run_dir, run_dir.name, rc, debug or "No debug output"

def HysplitRunner(max_workers: int = 4, limit: int | None = None):
    run_dirs = sorted([p for p in TEMP_ROOT.iterdir() if (p.is_dir() and p.name != "bdyfiles")])
    if limit is not None:
        run_dirs = run_dirs[:limit]

    print(f"Found {len(run_dirs)} run directories under {TEMP_ROOT}")
    print("Starting HYSPLIT runs...")

    ok = 0
    fail = 0

    with ProcessPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(run_hysplit_in_dir, rd) for rd in run_dirs]
        for fut in as_completed(futures):
            run_dir, name, rc, debug = fut.result()
            outfile = run_dir / name
            if rc == 0:
                ok += 1
                 # ---- site name (prefix before "_") ----
                site = name.split("_")[0]

                # ---- site directory inside traj_root ----
                site_dir = TRAJ_ROOT / site
                site_dir.mkdir(parents=True, exist_ok=True)

                # ---- move outfile if it exists ----
                if outfile and outfile.exists():
                    dest = site_dir / outfile.name
                    outfile.replace(dest)

                    print(f"[MOVE] {outfile.name}  →  {dest}")
                else:
                    print(f"[WARN] No outfile for successful run: {name}")

            else:
                fail += 1
                print(f"[FAIL] {name} (rc={rc})") # print if it fails
                print(debug.strip()[:800], "\n")

    print(f"\nAll done. Successful: {ok}, Failed: {fail}")

if __name__ == "__main__":

    workers = 12
    limit = None

    t0 = time.perf_counter()
    print(f"\n=== START {datetime.now()} | workers={workers} ===\n")

    HysplitRunner(max_workers=workers, limit=limit)

    dt = time.perf_counter() - t0
    print(f"\n=== END   {datetime.now()} | workers={workers} | elapsed={dt:.2f} s ===\n")


    

