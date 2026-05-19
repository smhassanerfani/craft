import os
import json
import glob
import calendar
import warnings
import argparse
import xarray as xr
import hdf5plugin
import dask
import dask.delayed
from tqdm import tqdm
from dask.distributed import Client, LocalCluster

# --- Constants (unchanged) ---
DIR_OUT_BASE = '/cw3e/mead/projects/cwp167/moerfani_data/regional'

VERTICAL_INDICES = [78, 69, 63, 58, 55, 51, 45, 40, 37, 35, 32, 29, 26, 23, 19, 15, 12, 10, 2]
LEVELS = [50, 100, 150, 200, 250, 300, 400, 500, 550, 600, 650, 700, 750, 800, 850, 900, 925, 950, 1000]

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, '..', 'notebooks', 'variable_config_wwrf.json')
SINCOS_PATH = os.path.join(SCRIPT_DIR, '..', 'notebooks', 'sincos.nc')

with open(CONFIG_PATH, 'r') as f:
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

SINCOS = xr.open_dataset(SINCOS_PATH, chunks='auto')
BDY = 5

COS_ALPHA = SINCOS['CosAlpha'].isel(time=0).values
SIN_ALPHA = SINCOS['SinAlpha'].isel(time=0).values

WIND_PAIRS = [('u', 'v'), ('10u', '10v'), ('ivt_u', 'ivt_v')]


# --- Cheap check: stays eager, runs instantly in the loop ---
def files_exist(date_str, dest_model, dest_single):
    pattern_p = os.path.join(dest_model,  f'wwrf_reanalysis_modellev_d01_{date_str}*.nc')
    pattern_s = os.path.join(dest_single, f'wwrf_reanalysis_singlelev_d01_{date_str}*.nc')
    return bool(glob.glob(pattern_p)) and bool(glob.glob(pattern_s))


# --- All heavy work is now fully deferred ---
@dask.delayed
def process_day(date_str, dest_model, dest_single, dir_out):
    pattern_p = os.path.join(dest_model,  f'wwrf_reanalysis_modellev_d01_{date_str}*.nc')
    pattern_s = os.path.join(dest_single, f'wwrf_reanalysis_singlelev_d01_{date_str}*.nc')

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=xr.SerializationWarning)

        dsp = xr.open_mfdataset(
            pattern_p,
            engine='h5netcdf',
            concat_dim='time',
            combine='nested',
            data_vars='minimal',
            coords='minimal',
            compat='override',
            chunks={'time': 1}
        ).isel(eta=VERTICAL_INDICES)[PRESSURE_VARS]

        dss = xr.open_mfdataset(
            pattern_s,
            engine='h5netcdf',
            concat_dim='time',
            combine='nested',
            data_vars='minimal',
            coords='minimal',
            compat='override',
            chunks={'time': 1}
        )[SINGLE_VARS]

        ds_combined = xr.merge([dsp, dss], compat='override')

        precip_resampled = ds_combined['precip_bkt'].resample(time='6h').sum()
        others_resampled = ds_combined.drop_vars('precip_bkt').resample(time='6h').mean()

        ds_resampled = xr.merge([others_resampled, precip_resampled], compat='override')
        ds_resampled = ds_resampled.rename(RENAME_MAP)

        ds_resampled = ds_resampled.isel(
            south_north=slice(BDY, -BDY),
            west_east=slice(BDY, -BDY)
        )

        ds_resampled = ds_resampled.rename({'eta': 'level', 'south_north': 'y', 'west_east': 'x'})
        ds_resampled = ds_resampled.assign_coords(level=LEVELS)
        ds_resampled = ds_resampled.sortby('level', ascending=False)

        ds_resampled['lat'].attrs['standard_name'] = 'latitude'
        ds_resampled['lat'].attrs['units'] = 'degrees_north'
        ds_resampled['lon'].attrs['standard_name'] = 'longitude'
        ds_resampled['lon'].attrs['units'] = 'degrees_east'

        ds_resampled['lon'].attrs.pop('_CoordinateAxisType', None)
        if 'description' in ds_resampled['lon'].attrs:
            ds_resampled['lon'].attrs['long_name'] = ds_resampled['lon'].attrs.pop('description')

        ds_resampled['lat'].attrs.pop('_CoordinateAxisType', None)
        if 'description' in ds_resampled['lat'].attrs:
            ds_resampled['lat'].attrs['long_name'] = ds_resampled['lat'].attrs.pop('description')

        # Rotate wind components using pre-loaded numpy arrays
        # for u_key, v_key in WIND_PAIRS:
        #     u = ds_resampled[u_key]
        #     v = ds_resampled[v_key]
        #     ds_resampled[u_key] = u * COS_ALPHA - v * SIN_ALPHA
        #     ds_resampled[v_key] = v * COS_ALPHA + u * SIN_ALPHA

        encoding = {
            var: {'chunksizes': (1, *ds_resampled[var].shape[1:])}
            for var in ds_resampled.data_vars
            if 'time' in ds_resampled[var].dims
        }

        save_path = os.path.join(dir_out, f'wwrf_reanalysis_d01_{date_str}.nc')
        # ✅ compute=False is gone — the @dask.delayed wrapper handles laziness
        ds_resampled.to_netcdf(save_path, encoding=encoding, engine='h5netcdf')
        return save_path


def main(year, month, dest_model, dest_single):
    cluster = LocalCluster(n_workers=16, threads_per_worker=4)
    client = Client(cluster)
    print(f"Dask Dashboard: {client.dashboard_link}")

    dir_out = os.path.join(DIR_OUT_BASE, f'{year}', f'{month:02d}')
    os.makedirs(dir_out, exist_ok=True)

    _, num_days = calendar.monthrange(year, month)

    # ✅ Graph building is now instant — just glob checks + creating lightweight delayed objects
    delayed_writes = []
    skipped = []
    for day in tqdm(range(1, num_days + 1), desc=f"Building graph {year}-{month:02d}"):
        date_str = f"{year}-{month:02d}-{day:02d}"
        if files_exist(date_str, dest_model, dest_single):
            delayed_writes.append(process_day(date_str, dest_model, dest_single, dir_out))
        else:
            skipped.append(date_str)

    if skipped:
        print(f"Skipped {len(skipped)} days with missing files: {skipped}")

    print(f"Computing {len(delayed_writes)} days in parallel...")
    dask.compute(*delayed_writes)

    client.close()
    cluster.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process WRF reanalysis NetCDF files.")
    parser.add_argument('--year',       type=int, required=True)
    parser.add_argument('--month',      type=int, required=True)
    parser.add_argument('--model_dir',  type=str, required=True)
    parser.add_argument('--single_dir', type=str, required=True)
    args = parser.parse_args()

    main(args.year, args.month, args.model_dir, args.single_dir)