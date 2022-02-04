# SantanderMetGroup/datatools

smgdatatools stands for data tools from [SantanderMetGroup](https://github.com/SantanderMetGroup).

## etl.py

Generate virtual datasets for climate data. Supports [Kerchunk](https://github.com/fsspec/kerchunk/), [HDF5 VDS](https://docs.h5py.org/en/stable/vds.html) and [NcML](https://www.unidata.ucar.edu/software/netcdf-java/v4.6/ncml/Tutorial.html).

### Kerchunk

#### ERA5 from Amazon S3

See a description of the dataset [here](https://registry.opendata.aws/ecmwf-era5/).

```bash
echo 'https://s3.amazonaws.com/era5-pds/2020/01/data/air_pressure_at_mean_sea_level.nc
https://s3.amazonaws.com/era5-pds/2020/02/data/air_pressure_at_mean_sea_level.nc
https://s3.amazonaws.com/era5-pds/2020/01/data/sea_surface_temperature.nc
https://s3.amazonaws.com/era5-pds/2020/02/data/sea_surface_temperature.nc' | \
etl.py --db test.sqlite --collector hdf5chunk --hdf5-driver ros3 --aggregations air_pressure_at_mean_sea_level sea_surface_temperature --etl jinja -t era5-s3.json.j2 --dest test.json
```

**You need to remove the last comma from the `test.json` file!**

```python
import xarray

ds = xarray.open_dataset("reference://", engine="zarr", backend_kwargs={
                    "consolidated": False,
                    "storage_options": {"fo": 'test.json', "remote_protocol": "s3","remote_options": {"anon": True}}
                    })
print(ds)
```

#### CMIP6 from Pangeo and Google Cloud

See a description of the dataset [here](https://console.cloud.google.com/marketplace/details/noaa-public/cmip6).

```bash
echo 'gs://cmip6/CMIP6/CMIP/NCAR/CESM2-FV2/historical/r2i1p1f1/Amon/tas/gn/v20200226
gs://cmip6/CMIP6/CMIP/NCAR/CESM2-FV2/historical/r1i1p1f1/Amon/tas/gn/v20191120
gs://cmip6/CMIP6/CMIP/NCAR/CESM2-FV2/historical/r2i1p1f1/Amon/pr/gn/v20200226
gs://cmip6/CMIP6/CMIP/NCAR/CESM2-FV2/historical/r1i1p1f1/Amon/pr/gn/v20191120' | \
etl.py --db test.sqlite --collector zarr --aggregations tas pr --etl jinja -t gcs-cmip6.json.j2 --dest test.json
```

**You need to remove the last comma from the `test.json` file!**

```python
import xarray

ds = xarray.open_dataset("reference://", engine="zarr", backend_kwargs={
                    "consolidated": False,
                    "storage_options": {"fo": 'test.json', "remote_protocol": "gs","remote_options": {"anon": True}}
                    })
print(ds)
```

Be careful with the following:

- Number of chunks does not match between ensemble members for the same variable. Check this against the SQL database (eg. `select count(*) from variable inner join chunk on variable.id = chunk.variable_id where variable.name = VARIABLE_NAME group by variable.id`).

### HDF5 Virtual Dataset

```bash
find test/data -maxdepth 1 -type f -name '*.nc' | grep -v 'fx' | etl.py --db test.sqlite --collector nc --aggregations tas pr --etl new-common --dest test.h5 --coord-name variant_label --coord-values-attr variant_label
```

Open the virtual dataset with xarray:

```python
import xarray

ds = xarray.open_dataset("test.h5")
ds[["tas", "pr"]].mean()
```

### NcML

```bash
find test/data -maxdepth 1 -type f -name '*.nc' | grep -v 'fx' | etl.py --db test.sqlite --collector nc --aggregations tas pr --etl jinja -t time-ensemble.ncml.j2 --dest test.ncml
```

Open the generated XML file with your favourite editor. You may also use [ToolsUI](https://docs.unidata.ucar.edu/netcdf-java/current/userguide/toolsui_ref.html) or [climate4R](https://github.com/SantanderMetGroup/climate4R).
