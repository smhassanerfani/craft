import os
import json
import glob
import calendar
import warnings
import argparse
import xarray as xr
import hdf5plugin
import dask
from tqdm import tqdm
from dask.distributed import Client, LocalCluster

# --- Constants ---
DIR_OUT_BASE = '/cw3e/mead/projects/cwp167/moerfani_data/regional'

VERTICAL_INDICES = [78, 69, 63, 58, 55, 51, 45, 40, 37, 35, 32, 29, 26, 23, 19, 15, 12, 10, 2]

with open('../notebooks/variable_config_wwrf.json', 'r') as f:
    config = json.load(f)

PRESSURE_VARS = []
SINGLE_VARS = []
RENAME_MAP = {}

for short, info in config['surface_variables'].items():
    SINGLE_VARS.append(info['wwrf_name'])
    RENAME_MAP[info['wwrf_name']] = short

for short, info in config['pressure_variables'].items():
    PRESSURE_VARS.append(info['wwrf_name'])
    RENAME_MAP[info['wwrf_name']] = short

SINCOS = xr.open_dataset('../notebooks/sincos.nc', chunks='auto')
BDY = 5

# --- Load sin/cos once into numpy arrays (avoids recompute each day) ---
COS_ALPHA = SINCOS['CosAlpha'].isel(time=0).values
SIN_ALPHA = SINCOS['SinAlpha'].isel(time=0).values

WIND_PAIRS = [('u', 'v'), ('10u', '10v'), ('ivt_u', 'ivt_v')]


def process_day(date_str, dest_model, dest_single, dir_out):
    pattern_p = os.path.join(dest_model,  f'wwrf_reanalysis_modellev_d01_{date_str}*.nc')
    pattern_s = os.path.join(dest_single, f'wwrf_reanalysis_singlelev_d01_{date_str}*.nc')

    if not glob.glob(pattern_p) or not glob.glob(pattern_s):
        tqdm.write(f"Warning: Missing files for {date_str}. Skipping.")
        return None

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=xr.SerializationWarning)

        # Open only needed variables upfront to reduce I/O
        dsp = xr.open_mfdataset(
            pattern_p, engine='h5netcdf', data_vars='all',
            chunks={'time': 1}
        ).isel(eta=VERTICAL_INDICES)[PRESSURE_VARS]

        dss = xr.open_mfdataset(
            pattern_s, engine='h5netcdf', data_vars='all',
            chunks={'time': 1}
        )[SINGLE_VARS]

        ds_combined = xr.merge([dsp, dss], compat='override')

        precip_resampled = ds_combined['precip_bkt'].resample(time='6h').sum()
        others_resampled = ds_combined.drop_vars('precip_bkt').resample(time='6h').mean()

        ds_resampled = xr.merge([others_resampled, precip_resampled], compat='override')
        ds_resampled = ds_resampled.rename(RENAME_MAP)

        # Remove boundary points
        ds_resampled = ds_resampled.isel(
            south_north=slice(BDY, -BDY),
            west_east=slice(BDY, -BDY)
        )

        # Rotate wind components using pre-loaded numpy arrays
        for u_key, v_key in WIND_PAIRS:
            u = ds_resampled[u_key]
            v = ds_resampled[v_key]
            ds_resampled[u_key] = u * COS_ALPHA - v * SIN_ALPHA
            ds_resampled[v_key] = v * COS_ALPHA + u * SIN_ALPHA

        # Build encoding: chunk time=4 (all steps), full size on other dims
        encoding = {
            var: {
                'chunksizes': (1, *ds_resampled[var].shape[1:]),
                'zlib': True,
                'complevel': 1,
            }
            for var in ds_resampled.data_vars
            if 'time' in ds_resampled[var].dims
        }

        save_path = os.path.join(dir_out, f'wwrf_reanalysis_d01_{date_str}.nc')
        return ds_resampled.to_netcdf(save_path, encoding=encoding, engine='h5netcdf', compute=False)


def main(year, month, dest_model, dest_single):
    # --- Tuned Dask Cluster ---
    cluster = LocalCluster(n_workers=8, threads_per_worker=2, memory_limit='16GB')
    client = Client(cluster)
    print(f"Dask Dashboard: {client.dashboard_link}")

    dir_out = os.path.join(DIR_OUT_BASE, f'{year}', f'{month:02d}')
    os.makedirs(dir_out, exist_ok=True)

    _, num_days = calendar.monthrange(year, month)

    # --- Build all delayed writes ---
    delayed_writes = []
    for day in tqdm(range(1, num_days + 1), desc=f"Building graph {year}-{month:02d}"):
        date_str = f"{year}-{month:02d}-{day:02d}"
        write = process_day(date_str, dest_model, dest_single, dir_out)
        if write is not None:
            delayed_writes.append(write)

    # --- Execute all days in parallel ---
    print(f"Computing {len(delayed_writes)} days in parallel...")
    dask.compute(*delayed_writes)

    client.close()
    cluster.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process WRF reanalysis NetCDF files.")
    parser.add_argument('--year',       type=int, required=True, help="Year to process (e.g. 2019)")
    parser.add_argument('--month',      type=int, required=True, help="Month to process (1–12)")
    parser.add_argument('--model_dir',  type=str, required=True, help="Path to model-level (pressure) input files")
    parser.add_argument('--single_dir', type=str, required=True, help="Path to single-level input files")
    args = parser.parse_args()

    main(args.year, args.month, args.model_dir, args.single_dir)