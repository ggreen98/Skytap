"""
Skytap Controller
=================

The main orchestrator for the Skytap HYSPLIT trajectory pipeline.

This script manages the end-to-end workflow of generating air mass back trajectories.
It is designed to run indefinitely or for long periods by using a "Rolling Window" approach
to manage disk usage for large meteorological files.

Workflow:
1.  **Configuration**: Validates user settings in `Config.yaml`.
2.  **Initialization**: Ensures the master list of HRRR files (`ARLfilelist.txt`) exists.
3.  **Environment Setup**: Ensures HYSPLIT run directories and auxiliary files (`bdyfiles`) are present.
4.  **Processing Loop**:
    - **Download**: Fetches a small batch of Meteorological (ARL) files (e.g., 6 files).
    - **Execute**: Triggers `HYSPLIT_Controller.py` to calculate valid time windows and `HYSPLIT_Runner.py` to run the model.
    - **Cleanup**: Deletes temporary run data.
    - **Rotate**: Deletes the oldest meteorological files and downloads new ones to shift the window forward.

Usage:
    python Skytap_Controller.py [--yes] [--check]
"""

import argparse
import yaml
import subprocess
import sys
import shutil
import time
from pathlib import Path
import re
from datetime import datetime

# Local import
import ARL_download_controller


def _tlog(log_path: Path, message: str) -> None:
    """Append a timestamped line to the timing log and print it."""
    line = f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {message}"
    print(line, flush=True)
    with log_path.open("a") as f:
        f.write(line + "\n")


def load_config(config_path: Path = Path("Config.yaml"), ex_cfg: Path = Path("Example.yaml")) -> tuple[dict, dict]:
    """
    Loads the user configuration and the example configuration for validation.

    Args:
        config_path (Path): Path to the user's config file.
        ex_cfg (Path): Path to the example config file (used as a schema reference).

    Returns:
        tuple[dict, dict]: A tuple containing (user_config, example_config).

    Raises:
        FileNotFoundError: If the user config file does not exist.
    """
    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {config_path.resolve()} \nPlease create a Config.yaml based on Example.yaml.")
    with config_path.open() as f:
        cfg = yaml.safe_load(f) or {}
    with ex_cfg.open() as f:
        ex_cfg = yaml.safe_load(f) or {}
    return cfg, ex_cfg


def validate_config(cfg: dict, ex_cfg: dict, path: str = "") -> list[str]:
    """
    Validates the user configuration against the example configuration structure.
    Checks for missing keys to ensure the script doesn't crash mid-run.

    Args:
        cfg (dict): User configuration dictionary.
        ex_cfg (dict): Example/Schema configuration dictionary.
        path (str): Current path in the dictionary (used for recursion).

    Returns:
        list[str]: A list of missing keys.
    """
    exclude_keys = ['ex_site_1', 'ex_site_2']
    missing = []

    def get_keys(d: dict) -> list[str]:
        """Recursively extracts all keys from a dictionary."""
        key_list = []
        for key, val in d.items():
            key_list.append(key)

            if isinstance(val, dict):
                for subkey, subval in val.items():
                    key_list.append(subkey)

                    if isinstance(subval, dict):
                        for subsubkey in subval.keys():
                            key_list.append(subsubkey)
        return key_list

    expected_keys = list(set(get_keys(ex_cfg)))
    expected_keys = [m for m in expected_keys if m not in exclude_keys]

    cfg_keys = list(get_keys(cfg))
    missing = [m for m in expected_keys if m not in cfg_keys]

    if len(missing) == 0:
        print("\n📄 Loaded Config Settings:\n")
        for key, value in cfg.items():
            if isinstance(value, dict):
                print(f"  {key}:")
                for subkey, subvalue in value.items():
                    if isinstance(subvalue, dict):
                        print(f"    {subkey}:")
                        for subsubkey, subsubvalue in subvalue.items():
                            print(f"      {subsubkey}: {subsubvalue}")
                    else:
                        print(f"    {subkey}: {subvalue}")
            else:
                print(f"  {key}: {value}")

    else:
        print("\n❌ Configuration Error: Missing or invalid settings:")
        for m in missing:
            print(f"  - {m}")
        print("\nPlease update Config.yaml based on Example.yaml and try again.\n")
    
    return missing

def prompt_yes_no(question: str) -> bool:
    """
    Prompts the user for a Yes/No answer via the console.
    """
    while True:
        ans = input(question).strip().lower()
        if ans in ("y", "yes"):
            return True
        if ans in ("n", "no"):
            return False
        print("Please enter 'y' or 'n'.")


def load_state(state_path: Path) -> dict:
    """Loads the state file (YAML) to resume progress if interrupted."""
    if not state_path.exists():
        return {}
    with state_path.open() as f:
        return yaml.safe_load(f) or {}


def save_state(state_path: Path, state: dict) -> None:
    """Saves the state dictionary to a YAML file."""
    with state_path.open("w") as f:
        yaml.safe_dump(state, f, sort_keys=False)


def run_arl_downloader_once(state_path: Path,
                           downloader_script: str = "ARL_downloader_config.py",
                           force: bool = False) -> None:
    """
    Runs the ARL downloader setup script if it hasn't been run yet.
    This script generates the 'ARLfilelist.txt' containing all URLs to download.
    
    Args:
        state_path (Path): Path to the state file.
        downloader_script (str): Name of the script to run.
        force (bool): If True, runs the script even if the state file says it's done.
    """
    state = load_state(state_path)
    already_ran = bool(state.get("arl_downloader_ran", False))

    if already_ran and not force:
        print(f"\n⏭️  ARL downloader already ran (state: {state_path}). Skipping.\n"
              f"   Use --force-arl to run it again.")
        return

    print(f"\n🚚 Running ARL downloader: {downloader_script}\n")
    result = subprocess.run([sys.executable, downloader_script], check=False)

    if result.returncode != 0:
        print(f"\n❌ ARL downloader failed with exit code {result.returncode}. Not updating state.")
        sys.exit(result.returncode)

    state["arl_downloader_ran"] = True
    save_state(state_path, state)
    print(f"\n✅ ARL downloader finished. State saved to: {state_path}\n")


def delete_files(file_urls, directory="ARL_Files"):
    """
    Deletes the local files corresponding to the provided URLs.
    Used to clean up old meteorological data that is no longer needed.
    
    Args:
        file_urls (list): List of URL strings.
        directory (str): Directory where files are stored.
    """
    for url in file_urls:
        filename = url.split('/')[-1]
        file_path = Path(directory) / filename
        try:
            if file_path.exists():
                file_path.unlink()
                print(f"Deleted: {filename}")
            else:
                print(f"File not found for deletion: {filename}")
        except Exception as e:
            print(f"Error deleting {filename}: {e}")

def clean_hysplit_dirs(temp_dir):
    """
    Deletes all subdirectories in temp_HYSPLIT_config_dir.
    Crucially, it preserves the 'bdyfiles' directory which contains static data needed for runs.
    """
    p = Path(temp_dir)
    if not p.exists():
        return

    for item in p.iterdir():
        if item.is_dir():
            if "bdyfiles" in item.name:
                continue
            
            # If it's a run directory, delete it
            try:
                shutil.rmtree(item)
            except Exception as e:
                print(f"Error cleaning {item}: {e}")
    print("Cleaned temporary HYSPLIT run directories.")


def validate_arl_list(file_path: Path, start_date_str: str, end_date_str: str) -> bool:
    """
    Checks if the ARL file list covers the requested date range.
    Parses dates from the first and last URLs in the file to ensure they overlap
    with the config's start/end dates.
    """
    if not file_path.exists():
        return False
        
    try:
        with file_path.open("r") as f:
            lines = [line.strip() for line in f if line.strip()]
            
        if not lines:
            return False

        # Parse config dates
        req_start = datetime.strptime(start_date_str, "%Y-%m-%d")
        req_end = datetime.strptime(end_date_str, "%Y-%m-%d")
        
        # Regex to find YYYYMMDD in URLs (e.g., hysplit.20190613.00z...)
        # Matches 8 digits (2000-2099)
        date_pattern = re.compile(r"(20[0-9]{2}[0-1][0-9][0-3][0-9])")
        
        # Check first file date
        first_match = date_pattern.search(lines[0])
        last_match = date_pattern.search(lines[-1])
        
        if not first_match or not last_match:
            print("⚠️  Could not parse dates from ARL file list URLs.")
            return False
            
        file_start = datetime.strptime(first_match.group(1), "%Y%m%d")
        file_end = datetime.strptime(last_match.group(1), "%Y%m%d")
        
        if file_start > req_start or file_end < req_end:
            print(f"\n⚠️  Date mismatch in {file_path.name}:")
            print(f"   Config Requested: {req_start.date()} to {req_end.date()}")
            print(f"   File List Covers: {file_start.date()} to {file_end.date()}")
            return False
            
        return True

    except Exception as e:
        print(f"Error validating ARL list: {e}")
        return False

def setup_hysplit_dirs(cfg):
    """
    Ensures the temporary HYSPLIT directory exists and contains the necessary 'bdyfiles'.
    The 'bdyfiles' (boundary files) directory typically contains 'ASCDATA.CFG', which
    tells HYSPLIT where to find land use and terrain data.
    """
    temp_dir = Path(cfg.get("temp_HYSPLIT_config_dir", "Temp_HYSPLIT_Dirs"))
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    # Source bdyfiles (from HYSPLIT installation)
    hysplit_root = Path(cfg.get("hysplit", {}).get("working_dir", "hysplit"))
    src_bdy = hysplit_root / "bdyfiles"
    
    # Dest bdyfiles (in temp dir)
    dst_bdy = temp_dir / "bdyfiles"
    
    # Check for ASCDATA.CFG specifically — the directory can exist but be empty
    # (e.g. if Docker volume pre-created it before the copy ran).
    if not (dst_bdy / "ASCDATA.CFG").exists():
        if src_bdy.exists():
            if dst_bdy.exists():
                shutil.rmtree(dst_bdy)
            print(f"📂 Copying bdyfiles from {src_bdy} to {dst_bdy}...")
            shutil.copytree(src_bdy, dst_bdy)
        else:
            print(f"⚠️  Warning: Could not find HYSPLIT bdyfiles at {src_bdy}. Runs might fail.")

def ensure_hysplit_binaries(cfg):
    """
    Checks if HYSPLIT binaries are present. If missing, looks for a .tar.gz 
    archive in the hysplit folder and extracts it automatically.
    This handles the case where Linux binaries are downloaded on Windows 
    and need to be extracted inside the Linux container to preserve permissions.
    """
    hysplit_root = Path(cfg.get("hysplit", {}).get("working_dir", "hysplit"))
    exec_dir = hysplit_root / "exec"
    hyts_std = exec_dir / "hyts_std"

    if hyts_std.exists():
        return

    print("🔍 HYSPLIT binaries not found. Checking for archives...")
    
    # Look for any .tar.gz or .tgz file in the hysplit directory
    archives = list(hysplit_root.glob("*.tar.gz")) + list(hysplit_root.glob("*.tgz"))
    
    if not archives:
        print(f"❌ Error: No HYSPLIT binaries or archives found in {hysplit_root}")
        print("   Please place the HYSPLIT Linux .tar.gz file in the 'hysplit' folder.")
        sys.exit(1)

    archive_path = archives[0]
    print(f"📦 Found archive: {archive_path.name}. Extracting inside container...")
    
    try:
        # We use subprocess with 'tar' because it's available in the Docker image
        # and handles Linux file permissions/symlinks better than zipfile/tarfile modules on some hosts.
        subprocess.run(["tar", "-xvzf", str(archive_path), "-C", str(hysplit_root)], check=True)
        print("✅ Extraction complete.")
        
        # Verify again
        if not hyts_std.exists():
            # Sometimes tarballs have a nested folder structure like hysplit/hysplit/...
            # Let's try to find it and move it
            nested = list(hysplit_root.glob("**/exec/hyts_std"))
            if nested:
                actual_exec_parent = nested[0].parent.parent
                print(f"📂 Found nested HYSPLIT structure at {actual_exec_parent}. Moving files...")
                for item in actual_exec_parent.iterdir():
                    dest = hysplit_root / item.name
                    if dest.exists():
                        if dest.is_dir(): shutil.rmtree(dest)
                        else: dest.unlink()
                    shutil.move(str(item), str(hysplit_root))
                print("✅ Files moved to root 'hysplit' directory.")

    except Exception as e:
        print(f"❌ Failed to extract HYSPLIT: {e}")
        sys.exit(1)

def preflight_check(cfg: dict, config_missing: list) -> None:
    """
    Validates config and checks prerequisites without starting a pipeline run.
    Exits 0 if all checks pass, 1 if any fail.
    """
    ok = True
    print("\n=== Skytap Pre-flight Check ===\n")

    # 1. Config
    if not config_missing:
        print("  ✅ Config.yaml loaded and validated")
    else:
        print(f"  ❌ Config.yaml has {len(config_missing)} missing key(s) — see above")
        ok = False

    # 2. HYSPLIT binary or archive
    hysplit_root = Path(cfg.get("hysplit", {}).get("working_dir", "hysplit"))
    hyts_std = hysplit_root / "exec" / "hyts_std"
    archives = list(hysplit_root.glob("*.tar.gz")) + list(hysplit_root.glob("*.tgz"))
    if hyts_std.exists():
        print(f"  ✅ HYSPLIT binary found: {hyts_std}")
    elif archives:
        print(f"  ✅ HYSPLIT archive found (will extract on first run): {archives[0].name}")
    else:
        print(f"  ❌ HYSPLIT binary not found in {hysplit_root}/")
        print( "     → Download Linux Ubuntu 20.04.6 LTS from https://www.ready.noaa.gov/HYSPLIT_linuxtrial.php")
        print(f"     → Place the .tar.gz in the '{hysplit_root}/' folder")
        ok = False

    # 3. bdyfiles (only warn if HYSPLIT itself is present)
    src_bdy = hysplit_root / "bdyfiles"
    if (hyts_std.exists() or archives) and not (src_bdy / "ASCDATA.CFG").exists():
        print(f"  ⚠️  bdyfiles/ASCDATA.CFG not found at {src_bdy} — may be inside archive (OK)")

    # 4. Docker
    docker_result = subprocess.run(["docker", "info"], capture_output=True)
    if docker_result.returncode == 0:
        print("  ✅ Docker is running")
    else:
        print("  ❌ Docker is not running or not installed")
        print("     → Start Docker Desktop and try again")
        ok = False

    # 5. ARL file list
    arl_list = Path(cfg.get("text_file_dir", "txt_files")) / cfg.get("full_ARL_file_list", "ARLfilelist.txt")
    list_valid = validate_arl_list(arl_list, cfg['start_date'], cfg['end_date'])
    if list_valid:
        with arl_list.open() as f:
            count = sum(1 for line in f if line.strip())
        print(f"  ✅ ARL file list ready ({count} files, {cfg['start_date']} – {cfg['end_date']})")
    else:
        print(f"  ⚠️  ARL file list missing or outdated — run without --check to regenerate")

    print()
    if ok:
        print("✅ All checks passed. Ready to run.\n")
        sys.exit(0)
    else:
        print("❌ Some checks failed. Fix the issues above before running.\n")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Skytap Controller")
    parser.add_argument("--yes", "-y", action="store_true", help="Automatically answer 'yes' to all confirmation prompts.")
    parser.add_argument("--check", action="store_true", help="Validate config and check prerequisites without running.")
    args = parser.parse_args()

    cfg, ex_cfg = load_config()

    # --- 1. Validation ---
    missing = validate_config(cfg, ex_cfg)

    if args.check:
        preflight_check(cfg, missing)  # exits internally

    if not args.yes and not prompt_yes_no("\nProceed with this configuration? [y/n]: "):
        sys.exit(0)

    # --- 2. Check ARL File List ---
    arl_list_path = Path("txt_files/ARLfilelist.txt")
    
    # Check if list exists AND matches the configured date range
    list_valid = validate_arl_list(arl_list_path, cfg['start_date'], cfg['end_date'])
    
    if not list_valid:
        print(f"\n{arl_list_path} is missing or outdated.")
        if args.yes or prompt_yes_no("Run setup (ARL_downloader_config.py) to regenerate list? [y/n]: "):
            
            # Clean out old meteorological data to prevent STOP 900 errors with new dates
            met_dir = Path(cfg.get("hysplit", {}).get("met_dir", "ARL_Files"))
            if met_dir.exists():
                print(f"🧹 Cleaning old ARL data from {met_dir}...")
                for f in met_dir.iterdir():
                    if f.is_file() and not f.name.startswith('.'):
                        try:
                            f.unlink()
                        except Exception as e:
                            print(f"   Error deleting {f.name}: {e}")

            # Fix for potential config key issue if it was boolean in original code
            # Prefer 'state_file', fallback to 'configured' or default
            st_path = cfg.get("state_file") or cfg.get("configured")
            if isinstance(st_path, bool) or st_path is None:
                 st_path = "state.yaml"
            
            run_arl_downloader_once(state_path=Path(str(st_path)), force=True)
            if not arl_list_path.exists():
                print("Error: Setup failed to create the file list.")
                sys.exit(1)
        else:
            if not arl_list_path.exists():
                print("Cannot proceed without file list.")
                sys.exit(1)
            print("⚠️  Proceeding with existing (potentially mismatched) file list.")

    # --- 3. Read URLs ---
    with arl_list_path.open("r") as f:
        all_urls = [line.strip() for line in f if line.strip()]

    total_files = len(all_urls)
    print(f"\nTotal ARL files to process: {total_files}")
    
    # --- 3.5 Setup HYSPLIT Environment ---
    ensure_hysplit_binaries(cfg)
    setup_hysplit_dirs(cfg)

    # --- 4. Processing Loop ---
    # The pipeline uses a "Rolling Window" strategy:
    # 1. Start with an initial window of files (e.g., 6).
    # 2. Process all valid trajectories within this window.
    # 3. Advance the window by a 'step' (e.g., 4):
    #    - Delete the oldest 'step' files.
    #    - Download the next 'step' files.
    # 4. Repeat until all files are processed.

    # Pipeline settings
    pipeline_cfg = cfg.get("pipeline", {})
    window_size = pipeline_cfg.get("window_size", 6)
    step = pipeline_cfg.get("window_step", 4)

    # --- Timing log ---
    timing_log = Path(cfg.get("text_file_dir", "txt_files")) / "pipeline_timing.log"
    pipeline_start = time.perf_counter()
    _tlog(timing_log, f"=== PIPELINE START | {total_files} ARL files | window={window_size} step={step} ===")
    state_path_str = cfg.get("state_file", "state.yaml")
    state_path = Path(state_path_str)
    
    print(f"\n--- Pipeline Settings ---")
    print(f"Window Size: {window_size} files")
    print(f"Step Size:   {step} files")

    # Load state for resume capability
    state = load_state(state_path)
    saved_idx = state.get("current_idx", 0)
    current_idx = 0

    if saved_idx > 0:
        if saved_idx >= total_files:
            print(f"\nPrevious run finished (Index {saved_idx}/{total_files}). Resetting to start.")
            state["current_idx"] = 0
            save_state(state_path, state)
        else:
            print(f"\nFound saved progress at file index {saved_idx} ({all_urls[saved_idx].split('/')[-1]}).")
            if args.yes or prompt_yes_no("Resume from saved position? [y/n]: "):
                current_idx = saved_idx
            else:
                print("Resetting progress to start.")
                state["current_idx"] = 0
                save_state(state_path, state)

    # Initial batch setup (Resume-aware)
    # If resuming, we need to ensure the full window is available, not just the "next step" files.
    # aria2c will skip existing files, so it's safe to request the full window.
    batch_end = min(current_idx + window_size, total_files)
    first_batch = all_urls[current_idx:batch_end]
    
    met_dir_str = cfg.get("hysplit", {}).get("met_dir", "ARL_Files")
    
    if not first_batch:
        print("No files to download.")
        return

    print(f"\n=== Initial Batch: Downloading files {current_idx} to {batch_end} ===")
    _t = time.perf_counter()
    ARL_download_controller.download_arl_files(first_batch, download_dir=met_dir_str)
    _e = time.perf_counter() - _t
    _tlog(timing_log, f"DOWNLOAD  iter=0 (initial)  files={len(first_batch)}  elapsed={_e:.1f}s ({_e/60:.1f}m)")

    # The 'on_disk' list tracks what urls are currently downloaded
    on_disk_urls = list(first_batch)

    iteration = 1
    while True:
        # Sync the temp file list to ALL currently on-disk files before HYSPLIT_Controller reads it.
        # Bug fix: download_arl_files() only writes the newest batch to this file, but
        # HYSPLIT_Controller.get_date_range() needs the full window (kept + new files) to
        # compute the correct valid time range and avoid 12-hour gaps between iterations.
        temp_list_path = Path(cfg["text_file_dir"]) / cfg["temp_arl_file_list"]
        with temp_list_path.open("w") as f:
            for url in on_disk_urls:
                f.write(url + "\n")

        # Run HYSPLIT Logic
        print("\n--- Running HYSPLIT Controller ---")
        # We call the controller as a subprocess to ensure a clean state for each batch.
        _t = time.perf_counter()
        ret = subprocess.run([sys.executable, "HYSPLIT_Controller.py"])
        _e = time.perf_counter() - _t
        _tlog(timing_log, f"HYSPLIT   iter={iteration}  elapsed={_e:.1f}s ({_e/60:.1f}m)")

        # --- ETA ---
        files_covered = min(current_idx + window_size, total_files)
        pct = files_covered / total_files * 100
        elapsed_total = time.perf_counter() - pipeline_start
        eta_s = (elapsed_total / (files_covered / total_files)) - elapsed_total
        _tlog(timing_log, f"          progress={files_covered}/{total_files} ({pct:.0f}%)  ETA ~{eta_s/60:.0f}m ({eta_s/3600:.1f}h)")

        # --- Trajectory validation ---
        traj_root = Path(cfg.get("hysplit", {}).get("traj_root", "Trajectory_Files"))
        valid_ct = small_ct = 0
        if traj_root.exists():
            for _tf in traj_root.rglob("*"):
                if _tf.is_file() and not _tf.name.startswith("."):
                    if _tf.stat().st_size >= 500:
                        valid_ct += 1
                    else:
                        small_ct += 1
        _warn = "  ⚠️  SUSPECT FILES DETECTED" if small_ct else ""
        _tlog(timing_log, f"          trajectories total={valid_ct + small_ct}  valid={valid_ct}  suspect(<500B)={small_ct}{_warn}")

        if ret.returncode != 0:
            print("HYSPLIT_Controller.py reported an error.")
            if not prompt_yes_no("HYSPLIT run failed. Continue to next batch? [y/n]: "):
                sys.exit(1)

        print("\n✅ HYSPLIT processing complete. Trajectories saved. Starting cleanup...")

        # Cleanup HYSPLIT Dirs
        clean_hysplit_dirs(cfg.get("temp_HYSPLIT_config_dir", "Temp_HYSPLIT_Dirs"))

        # Check if we are done
        # We are done if the window reached the end of all_urls
        if current_idx + window_size >= total_files:
            print("All files processed.")
            state["current_idx"] = total_files
            save_state(state_path, state)
            _e = time.perf_counter() - pipeline_start
            _tlog(timing_log, f"=== PIPELINE END | total={_e:.1f}s ({_e/60:.1f}m) ({_e/3600:.2f}h) ===\n")
            break

        # Prepare for next loop
        # Identify files to delete (the 'step' oldest in the window)
        
        to_delete = on_disk_urls[:step]
        print(f"\n--- Cleaning up {len(to_delete)} old ARL files ---")
        delete_files(to_delete, directory=cfg.get("hysplit", {}).get("met_dir", "ARL_Files"))

        # Update window indices
        current_idx += step
        
        # SAVE STATE HERE
        state["current_idx"] = current_idx
        save_state(state_path, state)
        print(f"💾 Progress saved. Next start index: {current_idx}")

        # Identify new files to download
        # We want to maintain 'window_size' files on disk if possible.
        # Window: [current_idx ... current_idx + window_size]
        
        dl_start = current_idx + (window_size - step)
        dl_end = current_idx + window_size
        
        new_batch = all_urls[dl_start : dl_end]
        
        if not new_batch:
            print("No new files to download (end of list reached).")
            break

        print(f"\n=== Next Batch: Downloading {len(new_batch)} new files (Index {dl_start} to {dl_end}) ===")
        _t = time.perf_counter()
        ARL_download_controller.download_arl_files(new_batch, download_dir=met_dir_str)
        _e = time.perf_counter() - _t
        _tlog(timing_log, f"DOWNLOAD  iter={iteration}  files={len(new_batch)}  elapsed={_e:.1f}s ({_e/60:.1f}m)")

        # Update tracking
        # The files on disk are now the kept ones + new ones
        on_disk_urls = on_disk_urls[step:] + new_batch
        iteration += 1


if __name__ == "__main__":
    main()

