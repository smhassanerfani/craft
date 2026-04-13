import xarray as xr
import pandas as pd
from tqdm.auto import tqdm
import os
import json
from dask.distributed import Client
from utils import LoggerDecorator


@LoggerDecorator(log_file='preprocess_era5.log')
def process_era5_month(ds_model, ds_sfc, selected_vars, rename_map, config, year, month, output_dir):
    """Processes a single month using pre-loaded datasets and configurations."""
    
    end_day = pd.Period(f"{year}-{month:02d}").days_in_month
    days = pd.date_range(
        start=f"{year}-{month:02d}-01",
        end=f"{year}-{month:02d}-{end_day:02d}",
        freq='D'
    )

    os.makedirs(output_dir, exist_ok=True)
    print(f"Starting pipeline for {year}-{month:02d} ({len(days)} days)...")

    datasets_to_save = []
    paths_to_save = []

    for day in tqdm(days, desc=f"Saving {year}-{month:02d} Daily NetCDFs", leave=False):
        day_str = day.strftime('%Y-%m-%d')
        save_path = os.path.join(output_dir, f'era5_modellev_d01_{day_str}.nc')

        if os.path.exists(save_path):
            continue

        # Slice JUST the 24 hours for this specific day FIRST
        ar_native_day = ds_model.sel(time=day_str)
        ar_sfc_day    = ds_sfc.sel(time=day_str)

        ar_combined = xr.merge([
            ar_native_day,
            ar_sfc_day.drop_dims('level', errors='ignore'),
        ], compat='override')

        ds_combined = ar_combined[selected_vars].rename(rename_map)

        # Handle t_sfc as combination of skin_temperature and sea_surface_temperature
        ds_combined['t_sfc'] = ds_combined['sea_surface_temperature'].fillna(ds_combined['skin_temperature'])
        ds_combined['t_sfc'].attrs.clear() 
        
        ds_combined = ds_combined.drop_vars(['skin_temperature', 'sea_surface_temperature'])

        # Set surface attributes (completely overwriting old ones)
        for short, info in config['surface_variables'].items():
            if short in ds_combined.variables:
                # Using .copy() replaces the entire attribute dictionary with JUST the JSON attributes
                ds_combined[short].attrs = info['attributes'].copy()

        # Set pressure attributes (completely overwriting old ones)
        for short, info in config['pressure_variables'].items():
            if short in ds_combined.variables:
                # Replace the dictionary to drop all the old GRIB_* junk
                ds_combined[short].attrs = info['attributes'].copy()

        vertical_subset = [48, 60, 68, 74, 79, 83, 90, 96, 98, 101, 103, 105, 108, 111, 114, 118, 120, 123, 133]
        ds_combined = ds_combined.sel(hybrid=vertical_subset, method='nearest')

        # FIX 3: Resample on the single day — graph is now trivially small
        precip_resampled  = ds_combined['tp'].resample(time='6h').sum()
        others_resampled  = ds_combined.drop_vars('tp').resample(time='6h').mean()
        ds_day_resampled  = xr.merge([others_resampled, precip_resampled])

        # FIX 4: Rechunk to a single time chunk before writing.
        ds_day_resampled = ds_day_resampled.chunk({'time': 1})

        datasets_to_save.append(ds_day_resampled)
        paths_to_save.append(save_path)

    if datasets_to_save:
            print(f"Writing {len(datasets_to_save)} files to disk for {year}-{month:02d}...")
            xr.save_mfdataset(datasets_to_save, paths_to_save, engine='netcdf4')


if __name__ == "__main__":
    # 1. Start Dask Client ONCE for the entire run
    with Client(n_workers=8, threads_per_worker=4, memory_limit='8GB', dashboard_address=':8797') as client:
        print(f"Dask dashboard: {client.dashboard_link}")

        # 2. Open Zarr datasets ONCE
        print("Connecting to GCP Zarr stores...")
        ds_model = xr.open_zarr(
            'gs://gcp-public-data-arco-era5/ar/model-level-1h-0p25deg.zarr-v1',
            chunks={},
            storage_options=dict(token='anon')
        )
        ds_sfc = xr.open_zarr(
            'gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3',
            chunks={},
            storage_options=dict(token='anon')
        )

        # 3. Load variable configuration and build lists ONCE
        print("Loading configuration...")
        with open('../notebooks/variable_config.json', 'r') as f:
            config = json.load(f)

        selected_vars = []
        rename_map = {}

        for short, info in config['surface_variables'].items():
            if 'era5_name' in info:
                selected_vars.append(info['era5_name'])
                rename_map[info['era5_name']] = short
            elif 'era5_names' in info:
                selected_vars.extend(info['era5_names'])

        for short, info in config['pressure_variables'].items():
            selected_vars.append(info['era5_name'])
            rename_map[info['era5_name']] = short

        # 4. Iterate through years and months
        for year in range(1979, 2021):
            for month in range(1, 13):
                output_dir = f'/cw3e/mead/projects/cwp167/moerfani_data/global/{year}/{month:02d}/'
                process_era5_month(
                    ds_model=ds_model, 
                    ds_sfc=ds_sfc, 
                    selected_vars=selected_vars, 
                    rename_map=rename_map, 
                    config=config, 
                    year=year, 
                    month=month, 
                    output_dir=output_dir
                )
        
        print("Processing completely finished for all years/months!")