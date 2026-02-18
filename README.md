# Skytap

Skytap is a high-performance, automated pipeline for generating **12-hour backward HYSPLIT trajectories** using HRRR meteorological data.

It is designed to handle large-scale data processing by implementing a **rolling window** workflow: automatically downloading meteorological files, processing them in parallel, and cleaning up disk space as it progresses.

## Quick Start (Portable Mode)

Skytap is designed to be run as a portable tool. You do not need to install Python or any dependencies, only **Docker**.

1.  **Download the Launcher**:
    Download the `skytap` (Mac/Linux) or `skytap.bat` (Windows) file from this repository.
    
2.  **Configuration**:
    *   **Create Config.yaml**: Copy `Example.yaml` and rename the copy to `Config.yaml`. Open it to set your coordinates and dates.
    *   **Crucial Step**: You **must** create the `Config.yaml` file *before* running the launcher for the first time. If you run the launcher first, Docker may create a *folder* named `Config.yaml` by mistake, which you will need to delete.
    *   **HYSPLIT Binaries**: You **must** provide the **Linux (x86_64)** version of HYSPLIT. 
        1. Download the Linux (x86_64) distribution from the [NOAA HYSPLIT Download Page](https://www.ready.noaa.gov/HYSPLIT_linuxtrial.php).
        2. Place the downloaded `.tar.gz` file (e.g., `hysplit_linux.tar.gz`) directly into the `hysplit/` folder in this repository.
        3. **Do not extract it.** The Skytap container will automatically extract it with the correct Linux permissions when it starts. (If you prefer to extract it manually, ensure `./hysplit/exec/hyts_std` is a valid path).

3.  **Run**:
    *   **Windows**: Double-click `skytap.bat`.
    *   **Mac/Linux**: Run `./skytap` in your terminal.

The tool will automatically pull the latest image from the GitHub Container Registry and start processing.

## Prerequisites

*   **Docker Desktop**: Installed and running.
*   **Linux HYSPLIT Binaries**: These are **required** and are not included in the repository due to licensing. See the Quick Start section above for instructions on where to download and where to place the `.tar.gz` file.

## Troubleshooting

### "Config.yaml is a directory" error
If you receive an error saying `Config.yaml` is a directory, it means you ran the script before creating the configuration file. Docker automatically creates a folder when a requested file mount is missing. 
**To fix:** Delete the `Config.yaml` folder, create a new file named `Config.yaml` (using `Example.yaml` as a template), and run the script again.

## Output
*   **Trajectory Files**: Saved to `./Trajectory_Files/`.
*   **State**: Progress is saved to `state.yaml`.

## Development & Automation

This repository uses **GitHub Actions** to automatically build and push the Docker image. 
*   **Registry**: `ghcr.io/ggreen98/skytap:latest`
*   **Workflow**: `.github/workflows/docker-publish.yml`

Whenever code is pushed to the `main` branch, the image is updated, and all users running the portable launchers will receive the update automatically on their next run.