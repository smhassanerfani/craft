import os
import glob
import xarray as xr
import numpy as np
from tqdm.auto import tqdm

# 1. Define Paths and Directories
input_pattern = '/cw3e/mead/projects/cwp167/moerfani_data/regional/2019/01M/wwrf_reanalysis_modellev_d01_2019-01-*.nc'
output_dir = '/cw3e/mead/projects/cwp167/moerfani_data/regional/2019/01Ms'

# Ensure the output directory exists
os.makedirs(output_dir, exist_ok=True)

# Generate a sorted list of all files matching the pattern
input_files = sorted(glob.glob(input_pattern))

if not input_files:
    raise FileNotFoundError("No files found matching the input pattern.")

# 2. Define Arrays and Calculate Target Indices
eta_levels = np.array([
    9.9804688e-01, 9.9316406e-01, 9.8730469e-01, 9.8242188e-01,
    9.7656250e-01, 9.6972656e-01, 9.6386719e-01, 9.5703125e-01,
    9.4921875e-01, 9.4140625e-01, 9.3359375e-01, 9.2480469e-01,
    9.1601562e-01, 9.0722656e-01, 8.9746094e-01, 8.8671875e-01,
    8.7597656e-01, 8.6425781e-01, 8.5253906e-01, 8.4082031e-01,
    8.2812500e-01, 8.1445312e-01, 8.0078125e-01, 7.8613281e-01,
    7.7148438e-01, 7.5585938e-01, 7.4023438e-01, 7.2460938e-01,
    7.0800781e-01, 6.9042969e-01, 6.7285156e-01, 6.5527344e-01,
    6.3769531e-01, 6.1914062e-01, 6.0058594e-01, 5.8203125e-01,
    5.6250000e-01, 5.4394531e-01, 5.2441406e-01, 5.0585938e-01,
    4.8632812e-01, 4.6777344e-01, 4.4824219e-01, 4.2968750e-01,
    4.1113281e-01, 3.9257812e-01, 3.7500000e-01, 3.5742188e-01,
    3.3984375e-01, 3.2324219e-01, 3.0664062e-01, 2.9101562e-01,
    2.7539062e-01, 2.6074219e-01, 2.4609375e-01, 2.3242188e-01,
    2.1875000e-01, 2.0605469e-01, 1.9335938e-01, 1.8164062e-01,
    1.7089844e-01, 1.6015625e-01, 1.4941406e-01, 1.3964844e-01,
    1.3085938e-01, 1.2207031e-01, 1.1328125e-01, 1.0546875e-01,
    9.7534180e-02, 9.0332031e-02, 8.3557129e-02, 7.7087402e-02,
    7.0983887e-02, 6.5246582e-02, 5.9875488e-02, 5.4748535e-02,
    4.9926758e-02, 4.5471191e-02, 4.1259766e-02, 3.7353516e-02,
    3.3691406e-02, 3.0395508e-02, 2.7282715e-02, 2.4414062e-02,
    2.1789551e-02, 1.9348145e-02, 1.7150879e-02, 1.5075684e-02,
    1.3122559e-02, 1.1352539e-02, 9.7274780e-03, 8.2168579e-03,
    6.8206787e-03, 5.5313110e-03, 4.3411255e-03, 3.2424927e-03,
    2.2201538e-03, 1.2817383e-03, 4.1675568e-04
])

target_pressures_hpa = np.array([50, 100, 150, 200, 250, 300, 400, 500, 600, 700, 850, 925, 1000])
target_etas = target_pressures_hpa / 1013.25

# Find the nearest indices
nearest_indices = [np.abs(eta_levels - target).argmin() for target in target_etas]

# 3. Iterate, Subset, and Save
# tqdm provides the progress bar over the file list
for file_path in tqdm(input_files, desc="Subsetting NetCDF Files"):
    
    file_name = os.path.basename(file_path)
    save_path = os.path.join(output_dir, file_name)
    
    # Skip if file already exists (useful if the script gets interrupted)
    if os.path.exists(save_path):
        continue

    # Open single dataset with Dask chunking
    # chunks='auto' distributes the slicing and saving workload across your CPU cores
    with xr.open_dataset(file_path, engine='h5netcdf', chunks='auto') as ds:
        
        # Extract the target levels
        # IMPORTANT: ensure 'eta' matches your exact dimension name in the dataset
        ds_subset = ds.isel(eta=nearest_indices)
        
        # Save to the new directory
        ds_subset.to_netcdf(save_path, engine='h5netcdf')