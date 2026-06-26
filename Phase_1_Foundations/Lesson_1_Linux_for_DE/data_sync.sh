#!/bin/bash
# data_sync.sh
# Purpose: This script demonstrates how an architect automates data movement from a landing zone to a processing zone.

SOURCE_DIR="./landing_zone"
TARGET_DIR="./processing_zone"
LOG_FILE="transfer_$(date +%Y%m%d).log"

# Function for clean logging
log_msg() {
    echo "[$(date '+%H:%M:%S')] - $1" | tee -a "$LOG_FILE"
}

# 1. Check for incoming data
if [ ! -d "$SOURCE_DIR" ]; then
    log_msg "ERROR: Source directory $SOURCE_DIR not found!"
    exit 1
fi

# 2. Check if there are any .csv files
FILES=$(ls "$SOURCE_DIR"/*.csv 2>/dev/null)
if [ -z "$FILES" ]; then
    log_msg "INFO: No new data files found in $SOURCE_DIR."
    exit 0
fi

# 3. Securely move data (Idempotency check: process each file once)
log_msg "Starting data transfer..."
mkdir -p "$TARGET_DIR"

for file in $FILES; do
    filename=$(basename "$file")
    log_msg "Moving $filename to processing..."
    mv "$file" "$TARGET_DIR/"
    if [ $? -eq 0 ]; then
        log_msg "SUCCESS: $filename moved."
    else
        log_msg "FAILED: $filename could not be moved!"
    fi
done

log_msg "Task Finished."
