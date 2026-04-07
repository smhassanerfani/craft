#!/bin/bash

# ==============================================================================
# CONFIGURATION - FILL THESE OUT
# ==============================================================================
# Your alphanumeric Client ID from the Globus Developer Console
CLIENT_ID="f26bd063-2c3c-469f-9ebd-44ee9d1c3157" 

# Find these UUIDs in the Globus Web App under "Endpoints"
MERCED_ENDPOINT_ID="de258060-df3f-4eaa-b10e-3799d6e6604f"
AWARE_ENDPOINT_ID="63ab9e0e-2049-4aff-bfec-5dfb64aadc11"

# Path definitions
SOURCE_PATH="/40YearReanalysis/v2/output/modellev/d01/2019/02/"
DEST_PATH="/mead/projects/cwp167/moerfani_data/regional/2019/02/"
# ==============================================================================

echo "--- 1. Initiating Transfer from Merced to AWARE ---"

# Start the transfer. We use --recursive for the directory and --sync-level checksum 
# to ensure data integrity for your .nc files.
# We capture the Task ID to monitor it.
TASK_ID=$(globus transfer "$MERCED_ENDPOINT_ID:$SOURCE_PATH" "$AWARE_ENDPOINT_ID:$DEST_PATH" \
    --recursive \
    --label "WWRF_Data_Transfer_2019_02" \
    --sync-level checksum \
    --jmespath 'task_id' --format unix)

if [ -z "$TASK_ID" ]; then
    echo "Error: Failed to submit transfer task."
    exit 1
fi

echo "Transfer submitted. Task ID: $TASK_ID"

echo "--- 2. Waiting for Transfer to Complete ---"
# This command pauses the script until the transfer is finished.
# --heartbeat prints a '.' every few seconds so you know it's working.
globus task wait "$TASK_ID" --heartbeat

# Check if the transfer actually succeeded
STATUS=$(globus task show "$TASK_ID" --jmespath 'status' --format unix)

if [ "$STATUS" == "SUCCEEDED" ]; then
    echo "Success! All .nc files are now on AWARE."
    
    # --------------------------------------------------------------------------
    # --- 3. RUN YOUR PYTHON PROCESSING SCRIPT ---
    # --------------------------------------------------------------------------
    echo "Starting Python processing..."
    
    # UNCOMMENT AND EDIT THE LINE BELOW:
    # python3 /path/to/your/processing_script.py --input "$DEST_PATH"
    
    # --------------------------------------------------------------------------
    # --- 4. DELETE RAW FILES FROM MERCED ---
    # --------------------------------------------------------------------------
    # Only delete if the processing step (or at least the transfer) was successful.
    echo "Cleaning up raw files on Merced..."
    
    # WARNING: This deletes the directory and files permanently on Merced.
    # globus delete "$MERCED_ENDPOINT_ID:$SOURCE_PATH" --recursive
    
    echo "Workflow complete."
else
    echo "Transfer failed with status: $STATUS. Cleanup aborted to prevent data loss."
    exit 1
fi