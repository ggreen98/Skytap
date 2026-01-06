import os
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
import yaml
from typing import Sequence

CONFIG_PATH = Path("Config.yaml")

def load_config():
    with CONFIG_PATH.open() as f:
        return yaml.safe_load(f)

def get_met_files(ARL_temp_file):
    return [line.strip() for line in open(ARL_temp_file)]
    
# def write_setup(run_dir: Path):
#     # Minimal defaults; keep it tiny to avoid version-key issues
#     (run_dir / "SETUP.CFG").write_text("&SETUP\n/\n")

def get_date_range(met_files: list[str]) -> list[datetime]:
    """
    Compute the list of all valid hourly trajectory end times covered by a set of
    HRRR/HYSPLIT meteorological files.

    This function parses mixed-format HRRR/HYSPLIT filenames, extracts their nominal
    timestamp (YYYYMMDDHH), and determines which hourly trajectory end times are
    supported based on available meteorological coverage.

    Each meteorological file is assumed to provide 6 hours of usable 
    forecast/analysis data beyond its nominal timestamp. For 12-hour back trajectories,
    a trajectory ending at time T requires meteorological data from:
        T - 12 hours  →  T

    Therefore:
        earliest_valid_time = earliest_file_time + 12 hours
        latest_valid_time   = latest_file_time + 6 hours

    This function generates all hourly datetimes between these two limits.

    Parameters
    ----------
    met_files : list[str]
        List of file paths or filenames. Filenames must follow one of two formats:
        - "hysplit.YYYYMMDD.HHz.*"
        - "YYYYMMDD_HH-xx_hrrr"

    Returns
    -------
    list[datetime]
        A list of all valid trajectory end times (hourly) for which sufficient
        meteorological data exist to support 12-hour back trajectories.

    Notes
    -----
    - The 12-hour trajectory length is currently hard-coded but can be parameterized.
    - The 6-hour forecast/analysis extension reflects typical HRRR/HYSPLIT met-file coverage.
    """

    file_start_times = []
    for mf in met_files:
        if "hysplit." in mf:
            mf = mf[-19:]
            parts = mf.split(".")
            date_str = parts[1]      # yyyymmdd
            time_str = parts[2][:-1]      # hh
            dt = datetime.strptime(date_str + time_str, "%Y%m%d%H")
            file_start_times.append(dt)
        else:
             mf = mf[-19:] 
             parts = mf.split("_")
             date_str = parts[0]      # yyyymmdd
             time_str = parts[1][:2]      # hh
             dt = datetime.strptime(date_str + time_str, "%Y%m%d%H")
             file_start_times.append(dt)
    file_end_times = [date + timedelta(hours=5) for date in file_start_times]
    earliest_valid = min(file_start_times) + timedelta(hours=12) # adjust if you dont want 12H back trajectories
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

    Example dir name:
        Temp_HYSPLIT_Dirs/ECC_20250408_15

    Returns the Path to the created directory.
    """
    temp_root = Path(load_config().get('temp_HYSPLIT_config_dir', 'Temp_HYSPLIT_Dirs')) # this might need to be changed based on config.yaml just getting the path for temp files
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

    Parameters
    ----------
    run_dir : Path
        Directory where the CONTROL file will be written.
    start_time : datetime
        Trajectory start time (UTC) for HYSPLIT.
    lat, lon : float
        Starting latitude and longitude in decimal degrees.
    height_m : float
        Starting height above ground level (meters).
    duration_h : int
        Trajectory duration in hours (negative for backward trajectories).
    top_agl_m : float
        Top of the model domain in meters AGL.
    met_files : Sequence[str]
        List of meteorological file names (not full paths).
    met_dir : Path
        Directory where the meteorological files reside.
    vert_motion : int, optional
        Vertical motion method flag used by HYSPLIT (default 0).

    Returns
    -------
    Path
        Path to the written CONTROL file.
    """
    run_dir.mkdir(parents=True, exist_ok=True)
    ctl_path = run_dir / "CONTROL"

    with ctl_path.open("w") as f:
        # 1) Start time (UTC) - HYSPLIT is fine with 4-digit year
        f.write(f"{start_time:%Y %m %d %H}\n")

        # 2) Number of starting locations
        f.write("1\n")

        # 3) Starting location: lat lon height(m)
        #    Formatting is not super strict, just space-separated numbers.
        f.write(f"{lat:.3f} {lon:.3f} {height_m:.2f}\n")

        # 4) Duration (hours), negative = backward
        f.write(f"{duration_h}\n")

        # 5) Vertical motion method (default 0)
        f.write(f"{vert_motion}\n")

        # 6) Top of model domain (meters)
        f.write(f"{top_agl_m:.1f}\n")

        # 7) Number of met files
        met_files = sorted([mf for mf in os.listdir(met_dir) if 'hrrr' in mf])
        f.write(f"{len(met_files)}\n")

        # 8) For each met file: directory line, then filename line
        for mf in met_files:
            f.write("../../" + str(met_dir) + "/\n")
            f.write(mf + "\n")
        
        f.write("./" + "\n")
        f.write(str(run_dir).split("/")[-1])

    return ctl_path

def make_run_dirs(cfg, valid_datetimes):
    for site in cfg['site_hysplit_configs']:

        start = datetime.strptime(cfg['site_hysplit_configs'][site]['start_date'], "%Y-%m-%d")
        end   = datetime.strptime(cfg['site_hysplit_configs'][site]['end_date'], "%Y-%m-%d")

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
                    Path(cfg['hysplit']['met_dir'])
                    )
    
    print(f"All CONTROL files written for period {valid_datetimes[0]} to {valid_datetimes[-1]}")

def remove_run_dirs(temp_root: Path):
    for folder in os.listdir(cfg['temp_HYSPLIT_config_dir']):
        run_path = temp_root / folder
        if "bdyfiles" in str(run_path):
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

    cfg = load_config() # load config.yaml
    valid_datetimes = get_date_range(get_met_files(f'{cfg["text_file_dir"]}/{cfg["temp_arl_file_list"]}')) # get valid datetimes based on met files in ARL_temp_file_list.txt
    make_run_dirs(cfg, valid_datetimes) # make run dirs and write CONTROL files for each site and valid datetime so that they can be run through HYSPLIT in parallel
    subprocess.run(["python3", "HYSPLIT_Runner.py"]) # call HYSPLIT_Runner.py to run HYSPLIT for all created CONTROL files
    delmessage = remove_run_dirs(Path(cfg['temp_HYSPLIT_config_dir'])) # remove all temporary HYSPLIT run directories
    print(delmessage)


    # for folder in os.listdir(cfg['temp_HYSPLIT_config_dir']):
    #     run_path = Path(cfg['temp_HYSPLIT_config_dir']) / folder
    #     print(run_path)
    #     #print(f"Running HYSPLIT for {folder}...")
    #     # result = subprocess.run([cfg['hysplit']['exe_path'], "-i", "-c", str(run_path / "CONTROL")], cwd=run_path)

    #     # if result.returncode != 0:
    #     #     print(f"HYSPLIT run failed for {folder}.")
    #     # else:
    #     #     print(f"HYSPLIT run completed for {folder}.")
