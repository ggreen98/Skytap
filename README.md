# Skytap

Skytap is a high-performance, automated pipeline for generating **12-hour backward HYSPLIT trajectories** using HRRR meteorological data.

It is designed to handle large-scale data processing by implementing a **rolling window** workflow: automatically downloading meteorological files, processing them in parallel, and cleaning up disk space as it progresses.

## Quick Start (Portable Mode)

Skytap is designed to be run as a portable tool. You do not need to install Python or any dependencies, only **Docker**.

1.  **Download the Launcher**:
    Download the `skytap` (Mac/Linux) or `skytap.bat` (Windows) file from this repository.
    
2.  **Configuration**:
    *   Create a `Config.yaml` file in the same folder as the launcher (use `Example.yaml` as a template).
    *   Ensure you have a `hysplit/` folder containing the **Linux** version of HYSPLIT binaries.

3.  **Run**:
    *   **Windows**: Double-click `skytap.bat`.
    *   **Mac/Linux**: Run `./skytap` in your terminal.

The tool will automatically pull the latest image from the GitHub Container Registry and start processing.

## Features

*   **Rolling Window Workflow**: Maintains a small footprint of active meteorological files.
*   **Parallel Processing**: Runs multiple HYSPLIT instances concurrently.
*   **Zero-Install**: Runs entirely inside Docker via GHCR.
*   **Resume Capability**: Tracks progress in `state.yaml`.

## Prerequisites

*   **Docker Desktop** installed and running.
*   **Linux HYSPLIT Binaries**: These must be placed in `./hysplit/exec/` on your host machine.

## Output
*   **Trajectory Files**: Saved to `./Trajectory_Files/`.
*   **State**: Progress is saved to `state.yaml`.

## Development & Automation

This repository uses **GitHub Actions** to automatically build and push the Docker image. 
*   **Registry**: `ghcr.io/ggreen98/skytap:latest`
*   **Workflow**: `.github/workflows/docker-publish.yml`

Whenever code is pushed to the `main` branch, the image is updated, and all users running the portable launchers will receive the update automatically on their next run.