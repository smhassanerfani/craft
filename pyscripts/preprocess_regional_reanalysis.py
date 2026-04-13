import os
import glob
import calendar
import warnings
import argparse
import xarray as xr
import hdf5plugin
from tqdm import tqdm
from dask.distributed import Client

# --- Constants ---
DIR_OUT_BASE = '/cw3e/mead/projects/cwp167/moerfani_data/regional'

VERTICAL_INDICES = [78, 69, 63, 58, 55, 51, 45, 40, 37, 35, 32, 29, 26, 23, 19, 15, 12, 10, 2]

PRESSURE_VARS = ['Z_e', 'T_e', 'q_e', 'u_gr_e', 'v_gr_e']

SINGLE_VARS = [
    'slp', 'p_sfc', 'Td_2m', 'T_2m', 'T_sfc',
    'IWV', 'IVTV', 'IVTU', 'u_10m_gr', 'v_10m_gr',
    'precip_bkt', 'Z_sfc', 'LandMask'
]


def main(year, month, dest_model, dest_single):
    # --- Start Dask Cluster ---
    client = Client()
    print(f"View Dask Dashboard to track CPU usage: {client.dashboard_link}")

    # --- Output Directory ---
    dir_out = os.path.join(DIR_OUT_BASE, f'{year}', f'{month:02d}')
    os.makedirs(dir_out, exist_ok=True)

    _, num_days = calendar.monthrange(year, month)

    # --- Processing Loop ---
    for day in tqdm(range(1, num_days + 1), desc=f"Processing {year}-{month:02d}"):
        date_str = f"{year}-{month:02d}-{day:02d}"

        pattern_p = os.path.join(dest_model,  f'wwrf_reanalysis_modellev_d01_{date_str}*.nc')
        pattern_s = os.path.join(dest_single, f'wwrf_reanalysis_singlelev_d01_{date_str}*.nc')

        if not glob.glob(pattern_p) or not glob.glob(pattern_s):
            tqdm.write(f"Warning: Missing files for {date_str}. Skipping.")
            continue

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=xr.SerializationWarning)

            with xr.open_mfdataset(pattern_p, engine='h5netcdf', data_vars='all', chunks='auto') as dsp, \
                 xr.open_mfdataset(pattern_s, engine='h5netcdf', data_vars='all', chunks='auto') as dss:

                dsp = dsp.isel(eta=VERTICAL_INDICES)

                dsp_selected = dsp[PRESSURE_VARS]
                dss_selected = dss[SINGLE_VARS]

                ds_combined = xr.merge([dsp_selected, dss_selected], compat='override')

                precip_resampled  = ds_combined['precip_bkt'].resample(time='6h').sum()
                others_resampled  = ds_combined.drop_vars('precip_bkt').resample(time='6h').mean()

                ds_resampled = xr.merge([others_resampled, precip_resampled], compat='override')

                save_path = os.path.join(dir_out, f'wwrf_reanalysis_d01_{date_str}.nc')
                ds_resampled.to_netcdf(save_path)

    client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process WRF reanalysis NetCDF files.")
    parser.add_argument('--year',       type=int, required=True, help="Year to process (e.g. 2019)")
    parser.add_argument('--month',      type=int, required=True, help="Month to process (1–12)")
    parser.add_argument('--model_dir',  type=str, required=True, help="Path to model-level (pressure) input files")
    parser.add_argument('--single_dir', type=str, required=True, help="Path to single-level input files")
    args = parser.parse_args()

    main(args.year, args.month, args.model_dir, args.single_dir)