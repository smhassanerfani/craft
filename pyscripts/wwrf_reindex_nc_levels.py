import xarray as xr
from pathlib import Path

# Base directory containing 2012, 2013, ... 2023
base_dir = Path("/cw3e/mead/projects/cwp167/moerfani_data/regional")

LEVELS = [50, 100, 150, 200, 250, 300, 400, 500, 550, 600, 650, 700, 750, 800, 850, 900, 925, 950, 1000]

def process_file(nc_path: Path, out_dir: Path):
    ds = xr.open_dataset(nc_path)

    ds = ds.rename({'eta': 'level', 'south_north': 'y', 'west_east': 'x'})
    ds = ds.assign_coords(level=LEVELS)
    ds = ds.sortby('level', ascending=False)

    # 2. Add the strict CF attributes that Earthkit requires
    ds['lat'].attrs['standard_name'] = 'latitude'
    ds['lat'].attrs['units'] = 'degrees_north'

    ds['lon'].attrs['standard_name'] = 'longitude'
    ds['lon'].attrs['units'] = 'degrees_east'

    # 1. Remove _CoordinateAxisType (using pop() prevents crashes if the key isn't there)
    ds['lon'].attrs.pop('_CoordinateAxisType', None)

    # 2. Rename 'description' to 'long_name'
    if 'description' in ds['lon'].attrs:
        ds['lon'].attrs['long_name'] = ds['lon'].attrs.pop('description')

    # --- Fix Latitude (you will likely need to do the same here) ---
    ds['lat'].attrs.pop('_CoordinateAxisType', None)

    if 'description' in ds['lat'].attrs:
        ds['lat'].attrs['long_name'] = ds['lat'].attrs.pop('description')

    out_path = out_dir / nc_path.name          # same filename, year-level dir
    ds.to_netcdf(out_path)
    ds.close()
    print(f"  Saved → {out_path}")

for year_dir in sorted(base_dir.glob("[0-9][0-9][0-9][0-9]")):
    print(f"\nProcessing year: {year_dir.name}")

    for month_dir in sorted(year_dir.glob("[0-9][0-9]")):
        nc_files = sorted(month_dir.glob("*.nc"))
        print(f"  Month {month_dir.name}: {len(nc_files)} files")

        for nc_file in nc_files:
            process_file(nc_file, year_dir)   # ← output goes to year/, not month/