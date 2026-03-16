import xarray as xr
import pandas as pd
from tqdm.auto import tqdm
import os
from dask.distributed import Client

def process_era5_month(year, month, output_dir):

    with Client(n_workers=32, threads_per_worker=1, memory_limit='2GB', dashboard_address=':8797') as client:
        print(f"Dask dashboard: {client.dashboard_link}")

        ds_model = xr.open_zarr(
            'gs://gcp-public-data-arco-era5/ar/model-level-1h-0p25deg.zarr-v1',
            chunks={},
            storage_options=dict(token='anon'),
        )
        ds_sfc = xr.open_zarr(
            'gs://gcp-public-data-arco-era5/ar/full_37-1h-0p25deg-chunk-1.zarr-v3',
            chunks={},
            storage_options=dict(token='anon'),
        )

        rename_map = {
            "mean_sea_level_pressure": "slp",
            "surface_pressure": "p_sfc",
            "2m_dewpoint_temperature": "Td_2m",
            "2m_temperature": "T_2m",
            "skin_temperature": "T_sfc",
            "total_column_water_vapour": "IWV",
            "vertical_integral_of_eastward_water_vapour_flux": "IVTU",
            "vertical_integral_of_northward_water_vapour_flux": "IVTV",
            "10m_u_component_of_wind": "u_10m_gr",
            "10m_v_component_of_wind": "v_10m_gr",
            "total_precipitation": "precip_bkt",
            "geopotential": "Z_e",
            "temperature": "T_e",
            "specific_humidity": "q_e",
            "u_component_of_wind": "u_gr_e",
            "v_component_of_wind": "v_gr_e",
            "geopotential_at_surface": "Z_sfc",
        }

        end_day = pd.Period(f"{year}-{month:02d}").days_in_month
        days = pd.date_range(
            start=f"{year}-{month:02d}-01",
            end=f"{year}-{month:02d}-{end_day:02d}",
            freq='D'
        )

        os.makedirs(output_dir, exist_ok=True)
        print(f"Starting pipeline for {year}-{month:02d} ({len(days)} days)...")

        for day in tqdm(days, desc="Downloading & Saving Daily NetCDFs"):
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

            ds_subset = ar_combined[list(rename_map.keys())].rename(rename_map)

            # FIX 3: Resample on the single day — graph is now trivially small
            precip_resampled  = ds_subset['precip_bkt'].resample(time='6h').sum()
            others_resampled  = ds_subset.drop_vars('precip_bkt').resample(time='6h').mean()
            ds_day_resampled  = xr.merge([others_resampled, precip_resampled])

            # FIX 4: Rechunk to a single time chunk before writing.
            # Avoids fragmented I/O — writes 4 timesteps as one contiguous block.
            ds_day_resampled = ds_day_resampled.chunk({'time': 4})

            ds_day_resampled.to_netcdf(save_path, engine='netcdf4')

        print("Processing complete!")


if __name__ == "__main__":
    out_dir = '/cw3e/mead/projects/cwp167/moerfani_data/global/2019/01/'
    process_era5_month(2019, 1, out_dir)