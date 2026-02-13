"""
HYSPLIT Parallel Runner
=======================

Executes HYSPLIT model runs in parallel using `concurrent.futures.ProcessPoolExecutor`.

This script:
1.  Scans `Temp_HYSPLIT_Dirs` for prepared run directories (containing `CONTROL` files).
2.  Spawns worker processes to execute the HYSPLIT executable in each directory.
3.  Monitors execution for timeouts or errors.
4.  Moves successful output files to the `Trajectory_Files` directory.

Usage:
    Invoked by `HYSPLIT_Controller.py`.
"""

from __future__ import annotations
import os
import subprocess
import shutil
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import yaml
import time
from datetime import datetime

CONFIG_PATH = Path("Config.yaml")

def load_config():
    """Loads the YAML configuration file."""
    with CONFIG_PATH.open() as f:
        return yaml.safe_load(f)
    
cfg = load_config()

# Define Paths
TEMP_ROOT = Path(cfg["temp_HYSPLIT_config_dir"])
PROJECT_ROOT = Path(__file__).resolve().parent
HYSPLIT_EXE = (PROJECT_ROOT / cfg["hysplit"]["exec_path"]).resolve()
TRAJ_ROOT = (PROJECT_ROOT / cfg["hysplit"]["traj_root"]).resolve()

# Ensure output directory exists
TRAJ_ROOT.mkdir(parents=True, exist_ok=True)


if not HYSPLIT_EXE.exists():
    raise FileNotFoundError(f"HYSPLIT executable not found: {HYSPLIT_EXE}")

def run_hysplit_in_dir(run_dir: Path, timeout_s: int = 600) -> tuple[str, int, str]:
    """
    Executes HYSPLIT within a specific temporary directory.

    Args:
        run_dir (Path): The directory containing the CONTROL file.
        timeout_s (int): Maximum runtime in seconds before killing the process.

    Returns:
        tuple: (run_dir_name, return_code, debug_message)
    """
    start_ts = datetime.now().strftime("%H:%M:%S")
    print(f"[START {start_ts}] {run_dir.name}", flush=True)

    ctl = run_dir / "CONTROL"
    if not ctl.exists():
        return run_dir.name, 999, "Missing CONTROL"

    # HYSPLIT output filename is defined in CONTROL as the directory name
    output_filename = run_dir.name
    
    # Cleanup previous outputs to ensure we don't read old data on failure
    for fname in (output_filename, "tdump", "MESSAGE"):
        p = run_dir / fname
        if p.exists():
            p.unlink()

    stdout_path = run_dir / "run_stdout.txt"
    stderr_path = run_dir / "run_stderr.txt"
    
    env = os.environ.copy()

    # Run the executable
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
    
    # Check for the expected output file (named same as directory)
    outfile = run_dir / output_filename
    
    if rc == 0 and outfile.exists() and outfile.stat().st_size > 0:
        # Debugging: Check for suspiciously small files (header only)
        if outfile.stat().st_size < 500:
            print(f"[WARN] Output file {output_filename} is very small ({outfile.stat().st_size} bytes).", flush=True)
            msg_file = run_dir / "MESSAGE"
            if msg_file.exists():
                print(f"--- MESSAGE file content for {run_dir.name} ---", flush=True)
                print(msg_file.read_text(errors="ignore"), flush=True)
                print("------------------------------------------------", flush=True)

        print(f"[END   {end_ts}] {run_dir.name} (rc={rc})", flush=True)
        return run_dir, run_dir.name, 0, "OK"

    # Debugging failure
    debug = ""
    if stderr_path.exists():
        debug = stderr_path.read_text(errors="ignore")[-2000:]
    msg = run_dir / "MESSAGE"
    if (not debug) and msg.exists():
        debug = msg.read_text(errors="ignore")[-2000:]
    
    print(f"[FAIL/END {end_ts}] {run_dir.name} (rc={rc})", flush=True)
    return run_dir, run_dir.name, rc, debug or "No debug output or output file missing"

def HysplitRunner(max_workers: int = 4, limit: int | None = None):
    """
    Main runner function that orchestrates parallel HYSPLIT execution.

    Args:
        max_workers (int): Number of parallel processes.
        limit (int, optional): Limit the number of directories to process (for testing).
    """
    run_dirs = sorted([p for p in TEMP_ROOT.iterdir() if p.is_dir() and (p / "CONTROL").exists()])

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
                # Parsing site name from folder: e.g. "SiteA_20200101_12" -> "SiteA"
                site = name.split("_")[0]

                # Organize output into site-specific folders
                site_dir = TRAJ_ROOT / site
                site_dir.mkdir(parents=True, exist_ok=True)

                if outfile and outfile.exists():
                    dest = site_dir / outfile.name
                    # shutil.move handles cross-device links (e.g. between Docker volumes)
                    shutil.move(str(outfile), str(dest))

                    print(f"[MOVE] {outfile.name}  ->  {dest}")
                else:
                    print(f"[WARN] No outfile for successful run: {name}")

            else:
                fail += 1
                print(f"[FAIL] {name} (rc={rc})")
                print(debug.strip()[:800], "\n")

    print(f"\nAll done. Successful: {ok}, Failed: {fail}")

if __name__ == "__main__":

    # Default workers to 12 if not specified
    workers = cfg.get("hysplit", {}).get("max_workers", 12)
    limit = None 

    t0 = time.perf_counter()
    print(f"\n=== START {datetime.now()} | workers={workers} ===\n")

    HysplitRunner(max_workers=workers, limit=limit)

    dt = time.perf_counter() - t0
    print(f"\n=== END   {datetime.now()} | workers={workers} | elapsed={dt:.2f} s ===\n")
