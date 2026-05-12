#!/bin/bash
#SBATCH --job-name=z31km
#SBATCH --output=./out/zarr-31km_%j.out
#SBATCH --error=./error/zarr-31km_%j.error
#SBATCH --partition=shared-192
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --account=cwp167
#SBATCH --export=ALL
#SBATCH --array=0-19:1
#SBATCH --mem=16Gb
#SBATCH -t 24:00:00

#############################################################################
# Conda environment and working directory
source $(conda info --base)/etc/profile.d/conda.sh
conda activate anemoi-datasets

workdir=/cw3e/mead/projects/cwp167/moerfani_data/
cd ${workdir}
#############################################################################
# Select part and number of parts
num_parts=20
node_id=$SLURM_ARRAY_TASK_ID
parts=($(seq 1 ${num_parts}))
part=${parts[$((node_id))]}
echo "Part ${part} / ${num_parts}"
#############################################################################
# Build .zarr using anemoi-datasets: https://anemoi-datasets.readthedocs.io/en/latest/index.html

# anemoi-datasets init /home/moerfani/projects/craft/anemoi-datasets-global-config.yaml craft-era5-31km.zarr --overwrite
anemoi-datasets load craft-era5-31km.zarr --part ${part}/${num_parts}
# anemoi-datasets finalise craft-era5-31km.zarr # (once the N parts are completed)
# anemoi-datasets cleanup craft-era5-31km.zarr # (to eliminate temporary files)
# anemoi-datasets inspect craft-era5-31km.zarr
#############################################################################