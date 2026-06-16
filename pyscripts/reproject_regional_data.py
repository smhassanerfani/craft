import os
import xarray as xr
from tqdm import tqdm

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
SINCOS_PATH = os.path.join(SCRIPT_DIR, '..', 'notebooks', 'sincos.nc')

WIND_PAIRS = [('u', 'v'), ('10u', '10v'), ('ivt_u', 'ivt_v')]

def main():
    SINCOS = xr.open_dataset(SINCOS_PATH, chunks='auto')

    COS_ALPHA = SINCOS['CosAlpha'].isel(time=0).values
    SIN_ALPHA = SINCOS['SinAlpha'].isel(time=0).values

    years = range(2012, 2023)
    for year in tqdm(years, desc='Years', unit='yr'):
        for month in tqdm(range(1, 13), desc=f'{year}', unit='mo', leave=False):
            input_path = f'/cw3e/mead/projects/cwp167/moerfani_data/regional/{year}/wwrf_reanalysis_d01_{year}-{month:02d}-*.nc'
            output_path = f'/cw3e/mead/projects/cwp167/moerfani_data/regional_lat-lon/{year}/wwrf_reanalysis_d01_{year}-{month:02d}.nc'

            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            ds = xr.open_mfdataset(input_path, combine='by_coords')

            # Rotate wind components using pre-loaded numpy arrays
            for u_key, v_key in WIND_PAIRS:
                u = ds[u_key]
                v = ds[v_key]
                ds[u_key] = u * COS_ALPHA - v * SIN_ALPHA
                ds[v_key] = v * COS_ALPHA + u * SIN_ALPHA

            ds.to_netcdf(output_path, engine='h5netcdf')
            ds.close()

if __name__ == '__main__':
    main()