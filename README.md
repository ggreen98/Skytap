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
    *   **HYSPLIT Binaries**: You **must** provide the **Linux (x86_64)** version of HYSPLIT (available from NOAA). Because Skytap runs inside a Linux Docker container, it requires Linux binaries even if your computer is running Windows or macOS. 
        *   **Automatic Extraction (Recommended)**: Simply place the downloaded `hysplit_linux.tar.gz` (or similar) file directly into the `hysplit/` folder. The Skytap container will automatically extract it with the correct Linux permissions when it starts.
        *   **Manual Extraction**: If you prefer to extract it yourself, ensure the `exec/` folder is directly inside `./hysplit/` (e.g., `./hysplit/exec/hyts_std` should be a valid path).

3.  **Run**:
    *   **Windows**: Double-click `skytap.bat`.
    *   **Mac/Linux**: Run `./skytap` in your terminal.

The tool will automatically pull the latest image from the GitHub Container Registry and start processing.

## Prerequisites

*   **Docker Desktop**: Installed and running.
*   **Linux HYSPLIT Binaries**: These are **required** and are not included in the repository due to licensing. You must place the Linux (x86_64) version of HYSPLIT into the `./hysplit/` directory. If the extraction creates a sub-folder, make sure the `exec/` folder is directly inside `./hysplit/` (e.g., `./hysplit/exec/hyts_std` should be a valid path).

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