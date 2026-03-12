# CRAFT: California River Atmospheric Forecasting with Terrain-following MLWP

**Institution:** Center for Western Weather and Water Extremes (CW3E)

## Project Overview
This Machine Learning Weather Prediction (MLWP) project focuses on improving precipitation forecasts for atmospheric rivers and weather extremes in California. By leveraging terrain-following coordinates, the model is designed to accurately capture the complex orographic effects that drive heavy precipitation events along the West Coast.

## Source Data

### 1. 40-Year Reanalysis (v2)
The foundational dataset is a high-resolution regional reanalysis archive. Due to the extreme scale of the data (nearly 300TB), aggressive preprocessing and dimensional reduction are required before the data can be used for ML training.

* **Location:** `/data/projects/40YearReanalysis/v2` 
* **Temporal Coverage:** 26 years (September 1997 → April 2023)
* **Domains & Volume:**
  * `d01` (6km resolution) — 130TB
  * `d02` (2km resolution) — 161TB
* **Original State:** 99 eta levels, hourly frequency, with redundant variables.

### 2. ECMWF ERA5 (ARCO)
To provide the necessary large-scale atmospheric context and boundary conditions, global data is sourced from the ECMWF ERA5 dataset. We utilize the Analysis-Ready, Cloud-Optimized (ARCO) architecture for efficient access.

* **Source Repository:** [google-research/arco-era5](https://github.com/google-research/arco-era5)
* **Format:** Zarr store (via Google Cloud Storage)
* **Spatial Resolution:** 0.25° 
* **Vertical Resolution:** 137 native hybrid sigma-pressure levels

---
