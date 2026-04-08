#!/bin/bash

# Ensure your endpoints are defined before running the loop
MERCED_ENDPOINT_ID="de258060-df3f-4eaa-b10e-3799d6e6604f"
AWARE_ENDPOINT_ID="63ab9e0e-2049-4aff-bfec-5dfb64aadc11"

# ==============================================================================

# Loop through years 2012 to 2020
for YEAR in {2012..2020}; do
    # Loop through months 01 to 12
    for MONTH in {01..12}; do
        
        echo "========================================================================"
        echo "Processing Year: $YEAR, Month: $MONTH"
        echo "========================================================================"

        # Define all paths upfront so they can be used for both transferring and deleting
        SOURCE_MODEL="/40YearReanalysis/v2/output/modellev/d01/${YEAR}/${MONTH}/"
        DEST_MODEL="/mead/projects/cwp167/moerfani_data/regional/modellev/${YEAR}/${MONTH}/"
        
        SOURCE_SINGLE="/40YearReanalysis/v2/output/singlelev/d01/${YEAR}/${MONTH}/"
        DEST_SINGLE="/mead/projects/cwp167/moerfani_data/regional/singlelev/${YEAR}/${MONTH}/"

        # Flag to track if both transfers succeed for this month
        MONTH_SUCCESS=true

        # ----------------------------------------------------------------------
        # --- 1. DOWNLOAD BOTH DATA TYPES ---
        # ----------------------------------------------------------------------
        for DATATYPE in "modellev" "singlelev"; do

            # Route variables based on current loop iteration
            if [ "$DATATYPE" == "modellev" ]; then
                SOURCE_PATH="$SOURCE_MODEL"
                DEST_PATH="$DEST_MODEL"
            else
                SOURCE_PATH="$SOURCE_SINGLE"
                DEST_PATH="$DEST_SINGLE"
            fi

            # Check if the data already exists to avoid re-downloading
            if [ -d "$DEST_PATH" ] && [ "$(ls -A "$DEST_PATH" 2>/dev/null)" ]; then
                echo "Directory $DEST_PATH already exists and contains files. Skipping download for $DATATYPE."
                continue 
            fi

            echo "Creating directory: $DEST_PATH"
            mkdir -p "$DEST_PATH"

            echo "Initiating Transfer from Merced to AWARE ($DATATYPE)..."
            TASK_ID=$(globus transfer "$MERCED_ENDPOINT_ID:$SOURCE_PATH" "$AWARE_ENDPOINT_ID:$DEST_PATH" \
                --recursive \
                --label "WWRF_${DATATYPE}_${YEAR}_${MONTH}" \
                --sync-level checksum \
                --jmespath 'task_id' --format unix)

            if [ -z "$TASK_ID" ]; then
                echo "Error: Failed to submit transfer task for ${DATATYPE} ${YEAR}/${MONTH}."
                MONTH_SUCCESS=false
                break # Break out of the DATATYPE loop, failing this month
            fi

            globus task wait "$TASK_ID" --heartbeat

            STATUS=$(globus task show "$TASK_ID" --jmespath 'status' --format unix)

            if [ "$STATUS" == "SUCCEEDED" ]; then
                echo "Success! $DATATYPE files for ${YEAR}/${MONTH} downloaded."
            else
                echo "Transfer failed with status: $STATUS."
                MONTH_SUCCESS=false
                break # Break out of the DATATYPE loop, failing this month
            fi

        done # End of DATATYPE loop

        # ----------------------------------------------------------------------
        # --- 2. PROCESS AND DELETE (Runs only if both transfers succeeded) ---
        # ----------------------------------------------------------------------
        if [ "$MONTH_SUCCESS" = true ]; then
            echo "Both modellev and singlelev data are ready for ${YEAR}/${MONTH}."
            
            echo "--- Starting Python processing ---"
            # Pass BOTH destination paths to your Python script so it can process them together
            # python3 /path/to/your/processing_script.py \
            #    --model_dir "$DEST_MODEL" \
            #    --single_dir "$DEST_SINGLE"
            
            echo "--- Cleaning up raw files on Merced ---"
            # Delete both source paths on Merced permanently
            # globus delete "$MERCED_ENDPOINT_ID:$SOURCE_MODEL" --recursive
            # globus delete "$MERCED_ENDPOINT_ID:$SOURCE_SINGLE" --recursive
            
            echo "Workflow for ${YEAR}/${MONTH} fully complete."
        else
            echo "Warning: One or both transfers failed for ${YEAR}/${MONTH}."
            echo "Skipping Python processing and cleanup to prevent data loss. Moving to next month."
            # The script will naturally continue to the next MONTH iteration
        fi

    done # End of MONTH loop
done # End of YEAR loop

echo "========================================================================"
echo "All years and months processed successfully!"