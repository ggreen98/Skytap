import argparse
import pandas as pd
import numpy as np
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

    

    with open("config.yaml", "r") as f:
        con = yaml.safe_load(f)
        
        start_date = dt.datetime.strptime(con['start_date'], "%Y-%m-%d")
        end_date = dt.datetime.strptime(con['end_date'], "%Y-%m-%d")
        hrrr1_server = con['hrrr1_server']
        hrrr_server = con['hrrr_server']
        hrrr_v1_format = con['hrrr_v1_format']
        hrrr_format = con['hrrr_format']
        wanted_months = con['months']

    with open(outfile, "w") as f:
        current = start_date
        while current <= end_date:
            y = current.year
            m = current.month
            d = current.day
            ymd = f"{y:04d}{m:02d}{d:02d}"

            if current < dt.datetime(2019, 6, 12):
                server_dir = hrrr1_server
                vformat = 1
            else:
                server_dir = hrrr_server
                vformat = 2

            if m in wanted_months:
                for bb in range(4):
                    if vformat == 1:
                        url = f"{server_dir}/{y:04d}/{m:02d}/hysplit.{ymd}.{hrrr_v1_format[bb]}"
                    elif vformat == 2:
                        url = f"{server_dir}/{y:04d}/{m:02d}/{ymd}_{hrrr_format[bb]}"
                    f.write(url + "\n")

            current += dt.timedelta(days=1)

    print("ARLfilelist.txt created. This file contains all the URLs for HRRR ARL data you intend to download.")
