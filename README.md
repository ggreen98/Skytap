# Skytap

Skytap is a high-performance, automated pipeline for generating **12-hour backward HYSPLIT trajectories** using HRRR meteorological data.

It handles large-scale data processing with a **rolling window** workflow: automatically downloading meteorological files, running HYSPLIT in parallel across all configured sites and heights, and cleaning up disk space as it progresses.

## Quick Start (Portable Mode)

Skytap is designed to be run as a portable tool. You do not need to install Python or any dependencies, only **Docker**.

1.  **Download the Launcher**:
    Download `skytap` (Mac/Linux) or `skytap.bat` (Windows) from this repository.

2.  **Configuration**:
    *   **Create Config.yaml**: Copy `Example.yaml` and rename it `Config.yaml`. Edit it to set your sites, coordinates, and date range.
    *   **Crucial Step**: You **must** create `Config.yaml` *before* running the launcher. If you run it first, Docker may create a *folder* named `Config.yaml` by mistake, which you will need to delete.
    *   **HYSPLIT Binaries**: You **must** provide the **Linux Ubuntu 20.04.6 LTS** version of HYSPLIT.
        1. Download the **Linux Ubuntu 20.04.6 LTS** distribution from the [NOAA HYSPLIT Download Page](https://www.ready.noaa.gov/HYSPLIT_linuxtrial.php).
        2. Place the downloaded `.tar.gz` file (e.g., `hysplit_linux.tar.gz`) directly into the `hysplit/` folder.
        3. **Do not extract it.** The container extracts it automatically with the correct Linux permissions on first run.

3.  **Run**:
    *   **Windows**: Double-click `skytap.bat`.
    *   **Mac/Linux**: Run `./skytap` in your terminal.

The tool automatically pulls the latest image from the GitHub Container Registry and starts processing.

## Prerequisites

*   **Docker Desktop**: Installed and running.
*   **Linux HYSPLIT Binaries**: Required and not included due to licensing. See above for download and placement instructions.

## Configuration

Copy `Example.yaml` to `Config.yaml` and edit it. Key settings:

| Key | Description |
|---|---|
| `start_date` / `end_date` | Date range to process |
| `months` | Only download ARL files for these months |
| `pipeline.window_size` | ARL files kept on disk at once (default `6`) |
| `pipeline.window_step` | Files rotated per cycle (default `4`) |
| `hysplit.max_workers` | Parallel HYSPLIT processes (default `12`) |
| `hysplit.top_of_model` | Model top in meters AGL (default `15100`) |
| `site_hysplit_configs` | Dict of monitoring sites |

### Site Configuration

Each site entry supports the following fields:

```yaml
site_hysplit_configs:
  FTCW:
    name: "Fort Collins - West"
    lat: 40.592543
    lon: -105.141122
    start_heights: [5, 100, 500]   # Meters AGL — each height launches a separate trajectory
    duration: -12                  # Hours (negative = backward)
    start_date: "2015-01-01"
    end_date: "2026-01-01"
```

**`start_heights`** is a list — each value produces a separate trajectory per hour per site. For a single-height run, use `[5]`. The old scalar `start_height` key is still accepted for backward compatibility.

## Output

*   **Trajectory Files**: Saved to `./Trajectory_Files/{site}/`.
*   **State**: Pipeline resume progress saved to `state.yaml`.
*   **Timing Log**: `txt_files/pipeline_timing.log` — records download times, HYSPLIT runtimes, progress percentage, ETA, and a per-iteration count of valid vs. suspect trajectory files.

## Pre-flight Check

Before running for the first time (or on a new machine), you can validate your setup without starting any downloads or HYSPLIT runs:

```bash
python Skytap_Controller.py --check
```

This checks:
- `Config.yaml` loads and passes schema validation
- HYSPLIT binary or `.tar.gz` archive is present in `hysplit/`
- Docker is installed and running
- ARL file list exists and covers the configured date range

Exits 0 if everything is ready, 1 if anything is missing (with a clear message for each issue).

## Troubleshooting

### "Config.yaml is a directory" error
You ran the launcher before creating the config file. Docker creates a folder when a file mount is missing.
**Fix**: Delete the `Config.yaml` folder, create a proper `Config.yaml` from `Example.yaml`, and run again.

### STOP 900 errors
HYSPLIT `STOP 900` errors indicate mismatched meteorological data. This typically happens when you change date ranges without clearing old files.
**Fix**: Delete everything in `ARL_Files/` and rerun.

### "Exec format error"
The HYSPLIT binary is not a Linux binary. Ensure you mounted the Linux Ubuntu 20.04.6 LTS version.

## Testing

Unit tests cover the core pipeline logic (no Docker, HYSPLIT binaries, or network access required):

```bash
pip install pytest
pytest test_pipeline.py -v
```

Tests cover: `get_date_range` time window boundaries, per-site date filtering, `validate_config`, and the rolling window index arithmetic.

## Development & Automation

This repository uses **GitHub Actions** to automatically build and push the Docker image.
*   **Registry**: `ghcr.io/ggreen98/skytap:latest`
*   **Workflow**: `.github/workflows/docker-publish.yml`

Pushes to `main` update the image automatically. All users running the portable launchers receive the update on their next run.

**Local dev** (with docker-compose):
```bash
docker compose -f docker/docker-compose.yml up --build
```
