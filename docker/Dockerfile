FROM python:3.9-slim

# Install system dependencies
# aria2: for downloading ARL files
# libgfortran5: common runtime dependency for HYSPLIT binaries
# procps: for ps command if needed by scripts
RUN apt-get update && apt-get install -y \
    aria2 \
    libgfortran5 \
    procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first for caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Create necessary directories
RUN mkdir -p ARL_Files Trajectory_Files Temp_HYSPLIT_Dirs txt_files

# Ensure Downloader script is executable
RUN chmod +x Downloader.sh

# Default command
CMD ["python", "Skytap_Controller.py"]