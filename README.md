# Skytap

Skytap is a high-performance, automated pipeline for generating **12-hour backward HYSPLIT trajectories** using HRRR meteorological data.

It is designed to handle large-scale data processing by implementing a **rolling window** workflow: automatically downloading meteorological files, processing them in parallel, and cleaning up disk space as it progresses.

## Features

*   **Rolling Window Workflow**: Maintains a small footprint of active meteorological files (default ~6) while processing long time periods.
*   **Parallel Processing**: Maximizes CPU usage by running HYSPLIT instances concurrently.
*   **Automated Data Retrieval**: Automatically identifies and downloads the exact HRRR ARL files needed for your date range using `aria2c`.
*   **Containerized**: Runs completely inside Docker, pre-configured with HYSPLIT and necessary dependencies.
*   **Resume Capability**: Tracks progress and allows resuming interrupted runs.

## Prerequisites

*   **Docker Desktop** (or Docker Engine + Compose) installed on your machine.

## Installation & Setup

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/your-repo/skytap.git
    cd skytap
    ```

2.  **Configuration**:
    *   Create your config file:
        ```bash
        cp Example.yaml Config.yaml
        ```
    *   Edit `Config.yaml`:
        *   **Dates**: Set `start_date` and `end_date`.
        *   **Sites**: Define your sites (Lat, Lon, Start Height) in `site_hysplit_configs`.
        *   **Paths**: Generally, leave the default paths (`./ARL_Files`, etc.) as they are mapped into the Docker container.

## Usage

### Running with Docker

**Option 1: Automatic Run (Default)**
1.  **Start the pipeline**:
    ```bash
    docker compose -f docker/docker-compose.yml up --build
    ```
    This command builds the container and starts the `Skytap_Controller.py` script automatically.

2.  **Stopping**:
    *   Press `Ctrl+C` in the terminal to stop the container gracefully.

**Option 2: Interactive Mode (Manual Control)**
If you want to debug, run specific scripts, or explore the environment:

1.  **Start the container in the background**:
    ```bash
    # Overrides the default command to keep the container running
    docker compose -f docker/docker-compose.yml run --rm --entrypoint bash skytap
    ```

2.  **Run the controller manually**:
    Once inside the container shell:
    ```bash
    python Skytap_Controller.py
    ```

3.  **Run helper scripts**:
    You can also run individual components for testing:
    ```bash
    python ARL_download_controller.py  # Test the downloader
    python HYSPLIT_Controller.py       # Test setup logic
    ```

### Output
*   **Trajectory Files**: All generated trajectory (tdump) files are saved to the `Trajectory_Files/` directory on your host machine (mapped to the container).
*   **Logs**: Check the console output for progress updates.
*   **State Management**: The pipeline **automatically creates and maintains** a `state.yaml` file to track progress. If the process is interrupted, it will use this file to resume from the last successful batch. You do not need to create this file manually, but you can delete it to restart the pipeline from the beginning.

## How it Works

1.  **Initialization**: The script checks for `Config.yaml` and `txt_files/ARLfilelist.txt`.
2.  **Setup**: If the file list is missing, it generates the master list of all HRRR files needed for your date range.
3.  **Environment Check**: It ensures the `bdyfiles` (boundary data) required by HYSPLIT are present in the run environment.
4.  **Processing Loop**:
    *   **Download**: It downloads a batch of ARL files (default window size: 6).
    *   **Run**: Triggers `HYSPLIT_Controller.py` to generate trajectories for the time window covered by these files.
    *   **Cleanup**:
        *   Deletes temporary run directories.
        *   Deletes the oldest 4 ARL files from disk.
    *   **Advance**: Downloads the next 4 files to maintain the window and repeats until finished.

## Troubleshooting

*   **Missing Data Errors**: Ensure your `Config.yaml` `met_dir` points to `ARL_Files` (or wherever you are mounting your storage).
*   **Permission Errors**: If running on Linux/Mac, ensure the `Downloader.sh` script is executable (`chmod +x Downloader.sh`), though Docker handles this during build.
*   **HYSPLIT Errors**: Check the `Temp_HYSPLIT_Dirs/` (if mounted) or console logs for specific HYSPLIT error codes. Common issues include missing `ASCDATA.CFG` (handled automatically now) or corrupt ARL files.

## Project Structure

*   `Config.yaml`: Main configuration file.
*   `Skytap_Controller.py`: **Main entry point**. Orchestrates the entire pipeline.
*   `ARL_download_controller.py`: Helper module for managing downloads.
*   `HYSPLIT_Controller.py`: Prepares HYSPLIT runs (CONTROL files).
*   `HYSPLIT_Runner.py`: Executes HYSPLIT in parallel.
*   `Dockerfile` & `docker-compose.yml`: Container configuration.
