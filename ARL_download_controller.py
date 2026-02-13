"""
ARL Download Controller Module
==============================

This module handles the downloading of Meteorological ARL files using a helper shell script.
It is designed to be used as a library by the main Skytap Controller but can also be run
independently for testing purposes.

Functions:
    download_arl_files(urls, temp_list_path, download_dir): Writes a batch of URLs to a temp file and invokes the downloader script.

Usage (Standalone):
    python ARL_download_controller.py
"""

import subprocess
import os
import sys

def download_arl_files(urls, temp_list_path="txt_files/ARL_temp_file_list.txt", download_dir="ARL_Files"):
    """
    Orchestrates the download of a list of ARL files.

    This function performs the following steps:
    1. Writes the provided list of URLs to a temporary text file.
    2. Executes the external shell script `Downloader.sh` which uses aria2c to download the files.

    Args:
        urls (list of str): A list of URL strings pointing to the ARL files to download.
        temp_list_path (str, optional): The path to the temporary file where URLs will be written. 
                                        Defaults to "txt_files/ARL_temp_file_list.txt".
        download_dir (str, optional): The directory to download files to. Defaults to "ARL_Files".

    Raises:
        subprocess.CalledProcessError: If the shell script exits with a non-zero status.
    """
    if not urls:
        print("No URLs provided to download.")
        return

    # Write temp batch file
    try:
        with open(temp_list_path, "w") as t:
            for url in urls:
                t.write(url + "\n")
    except IOError as e:
        print(f"Error writing temp file list {temp_list_path}: {e}")
        return

    print(f"Queueing {len(urls)} files for download:")
    for u in urls:
        print("  -", u.split('/')[-1])

    # Run downloader.sh
    print("\nRunning Downloader.sh...")
    
    # Pass DEST_DIR to the shell script via environment variable
    env = os.environ.copy()
    env["DEST_DIR"] = download_dir
    
    result = subprocess.run(["bash", "Downloader.sh"], env=env)

    if result.returncode != 0:
        print("Downloader.sh failed.")
        # We generally don't exit here so the controller can decide what to do,
        # but raising an exception might be cleaner.
        raise subprocess.CalledProcessError(result.returncode, "Downloader.sh")
    
    # Verify files were downloaded
    # We assume 'urls' filenames match what lands in 'download_dir'
    missing_files = []
    for u in urls:
        fname = u.split('/')[-1]
        if not os.path.exists(os.path.join(download_dir, fname)):
            missing_files.append(fname)
    
    if missing_files:
        print(f"Error: The following files were not found in {download_dir} after download attempt:")
        for m in missing_files:
            print(f"  - {m}")
        raise FileNotFoundError("Download appeared successful but files are missing. Check network/URLs.")

    print("Download batch complete.")

if __name__ == "__main__":
    """
    Entry point for manual testing.
    Checks for the existence of the master file list and attempts to download the first file.
    """
    # Legacy behavior for manual runs:
    # Read first 1 file from ARLfilelist.txt (if available) for testing
    if not os.path.exists("txt_files/ARLfilelist.txt"):
        print("Error: ARLfilelist.txt not found. Run ARL_downloader_config.py first.")
        exit(1)

    with open("txt_files/ARLfilelist.txt", "r") as f:
        all_urls = [line.strip() for line in f if line.strip()]

    if not all_urls:
        print("File list is empty.")
        exit(0)

    # Test with just the first one if run manually
    print("Test mode: downloading first 1 file only.")
    download_arl_files(all_urls[:1])