"""
ARL File List Generator
=======================

This script generates the master list of HRRR ARL files to be downloaded.

It reads the configuration from `Config.yaml` and produces `txt_files/ARLfilelist.txt`,
which contains a list of URLs pointing to the required meteorological data on the NOAA ARL server.
The URLs are constructed based on the date range and HRRR versioning (pre/post 2019-06-12).

Usage:
    python ARL_downloader_config.py [--force]
"""

import argparse
import datetime as dt
import yaml
import sys
import os

def confirm_overwrite(path):
    """Ask user to confirm file overwrite."""
    print(f"⚠️  WARNING: '{path}' already exists!")
    resp = input("This will overwrite the existing ARL file list. Continue? (y/n): ").strip().lower()
    if resp not in ("y", "yes"):
        print("Cancelled.")
        sys.exit(0)  

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Generate ARLfilelist.txt for HRRR downloads."
    )
    
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite ARLfilelist.txt without asking."
    )

    args = parser.parse_args()

    outfile = "txt_files/ARLfilelist.txt"

    # ----------------------------------------------------
    # Safety check — block overwrite unless --force
    # ----------------------------------------------------
    if os.path.exists(outfile) and not args.force:
        confirm_overwrite(outfile)

    # Load configuration
    with open("Config.yaml", "r") as f:
        con = yaml.safe_load(f)
        
        start_date = dt.datetime.strptime(con['start_date'], "%Y-%m-%d")
        end_date = dt.datetime.strptime(con['end_date'], "%Y-%m-%d")
        hrrr1_server = con['hrrr1_server']
        hrrr_server = con['hrrr_server']
        hrrr_v1_format = con['hrrr_v1_format']
        hrrr_format = con['hrrr_format']
        wanted_months = con['months']

    # Generate URLs
    with open(outfile, "w") as f:
        current = start_date
        while current <= end_date:
            y = current.year
            m = current.month
            d = current.day
            ymd = f"{y:04d}{m:02d}{d:02d}"

            # HRRR format changed on 2019-06-12
            if current < dt.datetime(2019, 6, 12):
                server_dir = hrrr1_server
                vformat = 1
            else:
                server_dir = hrrr_server
                vformat = 2

            if m in wanted_months:
                for bb in range(4):
                    if vformat == 1:
                        # Old Format: hysplit.20190611.00z.hrrra
                        url = f"{server_dir}/{y:04d}/{m:02d}/hysplit.{ymd}.{hrrr_v1_format[bb]}"
                    elif vformat == 2:
                        # New Format: 20190613_00-05_hrrr
                        url = f"{server_dir}/{y:04d}/{m:02d}/{ymd}_{hrrr_format[bb]}"
                    f.write(url + "\n")

            current += dt.timedelta(days=1)

    print(f"✅ Created {outfile}")
    print("   This file contains all the URLs for HRRR ARL data to be downloaded.")
