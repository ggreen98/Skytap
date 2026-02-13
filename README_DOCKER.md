# Running Skytap in Docker

This project can be run inside a Docker container. This is useful for deploying on any system without manually managing Python dependencies or system tools like `aria2`.

## Prerequisites

1.  **Docker** and **Docker Compose** installed.
2.  A **Linux** distribution of HYSPLIT.
    *   **Note for macOS/Windows users**: The HYSPLIT binaries in your local `hysplit/exec` folder are likely specific to your OS (e.g., Mach-O for Mac). These **will not run** inside the Docker container (which is Linux).
    *   You must obtain the Linux version of HYSPLIT (registered version or trial) and extract it to a folder (e.g., `hysplit`).

## Setup

1.  **Configuration**:
    Ensure you have `Config.yaml` set up (copy from `Example.yaml` if needed).
    
    ```bash
    cp Example.yaml Config.yaml
    ```

2.  **Prepare HYSPLIT**:
    Place your Linux HYSPLIT distribution in the `hysplit` folder in the project root.
    
    Structure should look like:
    ```
    Skytap/
    ├── docker/
    │   ├── Dockerfile
    │   └── docker-compose.yml
    ├── hysplit/
    │   ├── exec/
    │   │   └── hyts_std  <-- Must be a Linux binary
    │   ├── bdyfiles/
    │   └── ...
    ├── Config.yaml
    └── ...
    ```

    *If your HYSPLIT folder is named differently, update the `docker/docker-compose.yml` volume mapping:*
    ```yaml
    - ../path/to/your/hysplit:/app/hysplit
    ```

3.  **Update Config.yaml**:
    Ensure `exec_path` in `Config.yaml` points to the standard internal path:
    ```yaml
    hysplit:
      exec_path: "./hysplit/exec/hyts_std"
    ```

## Running

1.  **Build and Run**:
    ```bash
    docker compose -f docker/docker-compose.yml up --build
    ```

2.  **Detached Mode**:
    To run in the background:
    ```bash
    docker compose -f docker/docker-compose.yml up -d
    ```

3.  **View Logs**:
    ```bash
    docker compose -f docker/docker-compose.yml logs -f
    ```

## Troubleshooting

*   **"Exec format error"**: This means the HYSPLIT binary is not a Linux binary. Check that you mounted the correct Linux distribution of HYSPLIT.
*   **Permission Denied**: Ensure `Downloader.sh` is executable and the HYSPLIT binaries have execute permissions.