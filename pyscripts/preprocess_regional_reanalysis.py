import os
import glob
import calendar
import warnings
import xarray as xr
import hdf5plugin
from tqdm import tqdm
from dask.distributed import Client

# --- Start Dask Cluster ---
# This automatically uses all available CPU cores and threads.
client = Client() 
print(f"View Dask Dashboard to track CPU usage: {client.dashboard_link}")

# --- Configuration ---
year = 2019
month = 1

base_dir = f'/cw3e/mead/projects/cwp167/moerfani_data/regional/{year}/{month:02d}'
dir_p = f'{base_dir}P/'
dir_s = f'{base_dir}S/'
dir_out = f'{base_dir}M/'
os.makedirs(dir_out, exist_ok=True)

pressure_vars = ['Z_e', 'T_e', 'q_e', 'u_gr_e', 'v_gr_e']
single_vars = ['slp', 'p_sfc', 'Td_2m', 'T_2m', 'T_sfc', 'IWV', 'IVTV', 'IVTU', 'u_10m_gr', 'v_10m_gr', 'precip_bkt', 'Z_sfc']

_, num_days = calendar.monthrange(year, month)

# --- Processing Loop ---
for day in tqdm(range(1, num_days + 1), desc=f"Processing {year}-{month:02d}"):
    date_str = f"{year}-{month:02d}-{day:02d}"
    
    pattern_p = os.path.join(dir_p, f'wwrf_reanalysis_modellev_d01_{date_str}*.nc')
    pattern_s = os.path.join(dir_s, f'wwrf_reanalysis_singlelev_d01_{date_str}*.nc')
    
    if not glob.glob(pattern_p) or not glob.glob(pattern_s):
        tqdm.write(f"Warning: Missing files for {date_str}. Skipping.")
        continue

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=xr.SerializationWarning)
        
        # Add chunks='auto' to enable Dask parallel processing
        with xr.open_mfdataset(pattern_p, engine='h5netcdf', data_vars='all', chunks='auto') as dsp, \
             xr.open_mfdataset(pattern_s, engine='h5netcdf', data_vars='all', chunks='auto') as dss:
            
            dsp_selected = dsp[pressure_vars]
            dss_selected = dss[single_vars]

            ds_combined = xr.merge([dsp_selected, dss_selected], compat='override')

            precip_resampled = ds_combined['precip_bkt'].resample(time='6h').sum()

            others = ds_combined.drop_vars('precip_bkt')
            others_resampled = others.resample(time='6h').mean()

            ds_resampled = xr.merge([others_resampled, precip_resampled], compat='override')

            # The actual computation happens here when saving to disk
            save_path = os.path.join(dir_out, f'wwrf_reanalysis_modellev_d01_{date_str}.nc')
            ds_resampled.to_netcdf(save_path)
            
# Close the client when the script is done
client.close()