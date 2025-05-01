nwpdownload is an extension of the [Herbie](https://herbie.readthedocs.io/)
python package, focused on downloading large datasets for forecast calibration
and long-term forecast evaluation.

This package is in an early stage of development, so expect bugs and breaking
changes.

# Example

```python
import pandas as pd
from nwpdownload.collection import NwpCollection
from dask.distributed import Client

# set up the dask workers
client = Client(processes=False, threads_per_worker=4,
                n_workers=1, memory_limit='2GB')
client.dashboard_link # view info about the tasks and workers

# describe the GEFS data to get
search_0p25 = '|'.join([
    ':TMP:2 m above ground:',
    ':DPT:2 m above ground:',
    ':PRES:surface:'
])
# define the spatial extent for subsetting
nyc_extent = (285.5, 286.5, 40, 41.5)
runs = pd.date_range(start=f"2021-04-01 12:00", periods=4, freq='D')
fxx = range(3, 24 * 8, 3) # 63 forecast hours going out 8 days

gefs_0p25 = NwpCollection(runs, 'gefs', 'atmos.25', search_0p25, fxx,
                          members=['avg'], save_dir='/mnt/nwp',
                          extent=nyc_extent)
gefs_0p25.collection_size() # estimate the complete download size
gefs_0p25.get_status() # summary of existing files
gefs_0p25.download() # download files in parallel with dask
```

# Installation

Install the package with

```sh
pip install git+https://github.com/ASRCsoft/nwpdownload
```

# Benchmarks
