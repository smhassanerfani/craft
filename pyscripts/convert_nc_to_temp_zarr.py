import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
import xesmf as xe
from scipy import stats
from tqdm import tqdm

years = [str(y) for y in range(1980, 2026)]
months = [f"{m:02d}" for m in range(1, 13)]

# Load pre-computed weights
WEIGHTS_BILINEAR = xr.open_dataset('../notebooks/weights_bilinear_n320.nc')
WEIGHTS_NEAREST = xr.open_dataset('../notebooks/weights_nearest_s2d_n320.nc')

# Load target grid coordinates
lats_n320 = np.load("../notebooks/N320-coords/lats_n320.npy")
lons_n320 = np.load("../notebooks/N320-coords/lons_n320.npy")
ds_out = {
    'lon': lons_n320,
    'lat': lats_n320
}

# Use the first year as a sample dataset for building regridders
first_year = years[0]
sample_ds = xr.open_mfdataset(
    f'/cw3e/mead/projects/cwp167/moerfani_data/global/{first_year}/01/era5_modellev_d01_{first_year}-01-01.nc',
    chunks={'time': 4, 'hybrid': 1, 'latitude': -1, 'longitude': -1}
)

sample_ds_renamed = sample_ds.rename({'hybrid': 'level', 'longitude': 'lon', 'latitude': 'lat'})
sample_ds_renamed = sample_ds_renamed.assign_coords(
    level=[50, 100, 150, 200, 250, 300, 400, 500, 550, 600, 650, 700, 750, 800, 850, 900, 925, 950, 1000]
)
sample_ds_renamed = sample_ds_renamed.sortby('level', ascending=False)

sample_ds_lsm = sample_ds_renamed[['lsm', 'tp']]
sample_ds_continuous = sample_ds_renamed.drop_vars(['lsm', 'tp', 'tisr'])

print("Building regridders using pre-computed weights...")
regridder_bilinear = xe.Regridder(
    sample_ds_continuous,
    ds_out,
    method='bilinear',
    locstream_out=True,
    periodic=True,
    weights=WEIGHTS_BILINEAR
)

regridder_nearest = xe.Regridder(
    sample_ds_lsm,
    ds_out,
    method='nearest_s2d',
    locstream_out=True,
    periodic=True,
    weights=WEIGHTS_NEAREST
)

sample_ds.close()

for year in tqdm(years, desc="Processing years"):
    zarr_path = f'/cw3e/mead/projects/cwp167/moerfani_data/global-intermediate-zarr/{year}.zarr'

    for month in months:
        print(f"Processing {year}-{month}...")

        ds = xr.open_mfdataset(
            f'/cw3e/mead/projects/cwp167/moerfani_data/global/{year}/{month}/*.nc',
            chunks={'time': 4, 'hybrid': 1, 'latitude': -1, 'longitude': -1}
        )

        ds_renamed = ds.rename({'hybrid': 'level', 'longitude': 'lon', 'latitude': 'lat'})
        ds_renamed = ds_renamed.assign_coords(
            level=[50, 100, 150, 200, 250, 300, 400, 500, 550, 600, 650, 700, 750, 800, 850, 900, 925, 950, 1000]
        )
        ds_renamed = ds_renamed.sortby('level', ascending=False)

        ds_discrete = ds_renamed[['lsm', 'tp']]
        ds_continuous = ds_renamed.drop_vars(['lsm', 'tp', 'tisr'])

        interpolated_continuous = regridder_bilinear(ds_continuous, keep_attrs=True)
        interpolated_discrete = regridder_nearest(ds_discrete, keep_attrs=True)
        interpolated_data = xr.merge([interpolated_continuous, interpolated_discrete])

        if month == '01':
            interpolated_data.to_zarr(
                zarr_path,
                zarr_format=2,
                mode='w',
                consolidated=True
            )
        else:
            interpolated_data.to_zarr(
                zarr_path,
                zarr_format=2,
                mode='a',
                append_dim='time',
                consolidated=True
            )

        ds.close()
        ds_renamed.close()
        interpolated_data.close()

print("All years processed and saved as separate Zarr stores.")