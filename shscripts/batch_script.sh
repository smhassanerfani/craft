#!/bin/bash
#SBATCH -A cwp167
#SBATCH -p shared-192
#SBATCH --nodes=1
#SBATCH --ntasks=64
#SBATCH --mem=128GB
#SBATCH -t 7-00:00:00
#SBATCH -J preprocess_era5

# Load basic modules
# module load intel/2023.2.4.31

# Initialize and activate the conda environment
source $(conda info --base)/etc/profile.d/conda.sh
conda activate vis

# Change directory to the project folder
cd /home/moerfani/projects/craft/pyscripts/

# Run the python script
python preprocess_era5.py