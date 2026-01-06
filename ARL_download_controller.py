import subprocess
import os
import yaml

if __name__ == "__main__":

    # ---------------------------
    # STEP 1 — Ensure file exists
    # ---------------------------
    if not os.path.exists("txt_files/ARLfilelist.txt"):
        print("Error: ARLfilelist.txt not found. Run ARL_downloader_config.py first.")
        exit(1)

    # ---------------------------
    # STEP 2 — Read main file list
    # ---------------------------
    with open("txt_files/ARLfilelist.txt", "r") as f:
        ARL_URLS = [line.strip() for line in f if line.strip()]

    # Stop if empty
    if len(ARL_URLS) == 0:
        print("All ARL files processed! Nothing left in ARLfilelist.txt.")
        exit(0)

    # ---------------------------
    # STEP 3 — Take the first 6
    # ---------------------------
    batch = ARL_URLS[:6]
    ARL_URLS = ARL_URLS[6:]  # remaining

    # ---------------------------
    # STEP 4 — Write temp batch file
    # ---------------------------
    with open("txt_files/ARL_temp_file_list.txt", "w") as t:
        for url in batch:
            t.write(url + "\n")

    print("Created ARL_temp_file_list.txt with these files:")
    for u in batch:
        print("  -", u)

    # ---------------------------
    # STEP 5 — Run downloader.sh
    # ---------------------------
    print("\nRunning Downloader.sh...")
    result = subprocess.run(["bash", "Downloader.sh"])

    if result.returncode != 0:
        print("Downloader.sh failed. Keeping remaining URLs unchanged.")
        exit(1)
    