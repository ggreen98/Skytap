"""
HYSPLIT Controller
==================

This module prepares the environment for HYSPLIT execution by generating the necessary configuration files.

Key Functions:
1.  **get_date_range**: Analyzes available ARL files to determine valid start times for trajectories.
    *   *Note*: Logic is optimized for 12-hour back trajectories.
2.  **make_run_dirs**: Creates a temporary directory for each trajectory run (per site, per time).
3.  **write_control**: Generates the 'CONTROL' file required by HYSPLIT for each run.

Usage:
    This script is typically invoked by `Skytap_Controller.py` but can be run standalone for debugging.
"""

import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
import yaml
from typing import Sequence

CONFIG_PATH = Path("Config.yaml")

def load_config():
    """Loads the YAML configuration file."""
    with CONFIG_PATH.open() as f:
        return yaml.safe_load(f)

def get_met_files(ARL_temp_file):
    """
    Reads the list of currently available meteorological files from the temp file.
    
    Args:
        ARL_temp_file (str): Path to the temporary file list.
        
    Returns:
        list[str]: List of filenames/URLs.
    """
    return [line.strip() for line in open(ARL_temp_file)]

def get_date_range(met_files: list[str]) -> list[datetime]:
    """
    Compute the list of all valid hourly trajectory end times covered by a set of
    HRRR/HYSPLIT meteorological files.

    Each meteorological file is assumed to provide 6 hours of usable 
    forecast/analysis data beyond its nominal timestamp. For 12-hour back trajectories,
    a trajectory ending at time T requires meteorological data from:
        T - 12 hours  ->  T

    Therefore:
        earliest_valid_time = earliest_file_time + 12 hours
        latest_valid_time   = latest_file_time + 6 hours

    Parameters
    ----------
    met_files : list[str]
        List of file paths or filenames. Filenames must follow one of two formats:
        - "hysplit.YYYYMMDD.HHz.*" (old format)
        - "YYYYMMDD_HH-xx_hrrr" (new format)

    Returns
    -------
    list[datetime]
        A list of all valid trajectory end times (hourly) for which sufficient
        meteorological data exist to support 12-hour back trajectories.
    """

    file_start_times = []
    for mf in met_files:
        if "hysplit." in mf:
            # Format: hysplit.20190613.00z.hrrra
            mf = mf[-19:]
            parts = mf.split(".")
            date_str = parts[1]      # yyyymmdd
            time_str = parts[2][:-1]      # hh
            dt = datetime.strptime(date_str + time_str, "%Y%m%d%H")
            file_start_times.append(dt)
        else:
             # Format: 20200614_12-17_hrrr
             mf = mf[-19:] 
             parts = mf.split("_")
             date_str = parts[0]      # yyyymmdd
             time_str = parts[1][:2]      # hh
             dt = datetime.strptime(date_str + time_str, "%Y%m%d%H")
             file_start_times.append(dt)
    
    if not file_start_times:
        return []

    # Calculate coverage
    file_end_times = [date + timedelta(hours=5) for date in file_start_times]
    
    # HARDCODED: 12-hour buffer for back trajectories.
    # To support dynamic durations, this logic would need to use cfg['site']['duration'].
    earliest_valid = min(file_start_times) + timedelta(hours=12) 
    latest_valid = max(file_end_times)

    valid_hours = []
    t = earliest_valid
    while t <= latest_valid:
        valid_hours.append(t)
        t += timedelta(hours=1)
    return valid_hours


def make_run_dir(site_abbr: str, start_time: datetime) -> Path:
    """
    Create a unique temporary HYSPLIT run directory for a given site + hour.
    Example: Temp_HYSPLIT_Dirs/ECC_20250408_15
    """
    temp_root = Path(load_config().get('temp_HYSPLIT_config_dir', 'Temp_HYSPLIT_Dirs'))
    run_name = f"{site_abbr}_{start_time:%Y%m%d_%H}"
    run_dir = temp_root / run_name
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def write_control(
    run_dir: Path,
    start_time: datetime,
    lat: float,
    lon: float,
    height_m: float,
    duration_h: int,
    met_dir: Path,
    top_agl_m: float = 10000.0,
    vert_motion: int = 0,
) -> Path:
    """
    Write a HYSPLIT CONTROL file for a single trajectory run.
    
    The CONTROL file tells HYSPLIT:
    1. Start time
    2. Start location
    3. Run duration
    4. Meteorology file locations
    5. Output file name

    Returns
    -------
    Path
        Path to the written CONTROL file.
    """
    run_dir.mkdir(parents=True, exist_ok=True)
    ctl_path = run_dir / "CONTROL"

    with ctl_path.open("w") as f:
        # 1) Start time (UTC)
        f.write(f"{start_time:%Y %m %d %H}\n")

        # 2) Number of starting locations (1)
        f.write("1\n")

        # 3) Starting location: lat lon height(m)
        f.write(f"{lat:.3f} {lon:.3f} {height_m:.2f}\n")

        # 4) Duration (hours), negative = backward
        f.write(f"{duration_h}\n")

        # 5) Vertical motion method (0 = use data vertical velocity)
        f.write(f"{vert_motion}\n")

        # 6) Top of model domain (meters)
        f.write(f"{top_agl_m:.1f}\n")

        # 7) Meteorological files
        met_files = sorted([mf for mf in os.listdir(met_dir) if 'hrrr' in mf])
        if not met_files:
             raise FileNotFoundError(f"No ARL data files found in {met_dir}. HYSPLIT cannot run without met data.")
        
        f.write(f"{len(met_files)}\n")

        # 8) For each met file: directory line, then filename line
        for mf in met_files:
            f.write("../../" + str(met_dir) + "/\n")
            f.write(mf + "\n")
        
        # 9) Output directory (current dir)
        f.write("./" + "\n")
        # 10) Output filename (same as run directory name)
        f.write(str(run_dir).split("/")[-1])

    return ctl_path

def make_run_dirs(cfg, valid_datetimes):
    """
    Iterates through all sites in config and creates run directories for 
    every valid time slot.
    """
    for site in cfg['site_hysplit_configs']:

        start = datetime.strptime(cfg['site_hysplit_configs'][site]['start_date'], "%Y-%m-%d")
        end   = datetime.strptime(cfg['site_hysplit_configs'][site]['end_date'], "%Y-%m-%d")

        # Only create runs if the site's date range overlaps with the valid met data
        if start <= valid_datetimes[0] and end >= valid_datetimes[-1]:
            print(f"Writing HYSPLIT CONTROL files for site: {site}")

            for dt in valid_datetimes:
                write_control(
                    make_run_dir(site, dt),
                    dt,
                    cfg['site_hysplit_configs'][site]['lat'],
                    cfg['site_hysplit_configs'][site]['lon'],
                    cfg['site_hysplit_configs'][site]['start_height'],
                    cfg['site_hysplit_configs'][site]['duration'],
                    Path(cfg['hysplit']['met_dir']),
                    top_agl_m=cfg['hysplit'].get('top_of_model', 10000.0),
                    vert_motion=cfg['hysplit'].get('vert_motion', 0)
                    )
    
    print(f"All CONTROL files written for period {valid_datetimes[0]} to {valid_datetimes[-1]}")

def remove_run_dirs(temp_root: Path):
    """
    Removes temporary run directories after execution.
    Skips 'bdyfiles' which are static resources.
    """
    for folder in os.listdir(temp_root):
        run_path = temp_root / folder
        if "bdyfiles" in str(run_path) or folder.startswith('.'):
            continue
        else:
            for file in os.listdir(run_path):
                file_path = run_path / file
                try:
                    if file_path.is_file():
                        file_path.unlink()
                except Exception as e:
                    print(f"Error deleting file {file_path}: {e}")
            try:
                run_path.rmdir()
            except Exception as e:
                print(f"Error deleting directory {run_path}: {e}")
    return "All temporary HYSPLIT run directories removed."


if __name__ == "__main__":

    cfg = load_config()
    
    # 1. Get available met data list
    met_list_path = f'{cfg["text_file_dir"]}/{cfg["temp_arl_file_list"]}'
    
    # 2. Calculate valid time windows
    valid_datetimes = get_date_range(get_met_files(met_list_path))
    
    if not valid_datetimes:
        print("No valid time windows found for the current ARL files.")
    else:
        # 3. Create run directories and CONTROL files
        make_run_dirs(cfg, valid_datetimes)
        
        # 4. Trigger the parallel runner
        subprocess.run([sys.executable, "HYSPLIT_Runner.py"])
        
        # 5. Cleanup
        delmessage = remove_run_dirs(Path(cfg['temp_HYSPLIT_config_dir']))
        print(delmessage)