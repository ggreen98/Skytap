@echo off
:: Skytap Launcher for Windows
:: Uses image from GitHub Container Registry (GHCR)

set IMAGE=ghcr.io/ggreen98/skytap:latest

:: Get the directory where this script is located
cd /d %~dp0

:: Check if Docker is running
docker info >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Error: Docker is not running.
    echo Please start Docker Desktop and try again.
    pause
    exit /b 1
)

:: Pull the latest image
echo 🔄 Checking for updates...
docker pull %IMAGE%

:: Run the container
docker run -it --rm ^
  --platform linux/amd64 ^
  -v "%cd%/Config.yaml:/app/Config.yaml" ^
  -v "%cd%/state.yaml:/app/state.yaml" ^
  -v "%cd%/ARL_Files:/app/ARL_Files" ^
  -v "%cd%/Trajectory_Files:/app/Trajectory_Files" ^
  -v "%cd%/txt_files:/app/txt_files" ^
  -v "%cd%/Temp_HYSPLIT_Dirs:/app/Temp_HYSPLIT_Dirs" ^
  -v "%cd%/hysplit:/app/hysplit" ^
  -e PYTHONUNBUFFERED=1 ^
  %IMAGE% python Skytap_Controller.py --yes

if %errorlevel% neq 0 (
    echo.
    echo Pipeline exited with an error.
    pause
)