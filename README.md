# SantanderMetGroup/publisher

Tool for generation of NcMLs and TDS catalogs.

`ncml.py` reads file paths from stdin and generates one or more NcMLs.

`catalog.py` reads file paths from stdin and generates one or more TDS catalogs.

Configurations for multiple setups require to implement the 'Adapter' interface for both ncml and catalog (see `ncml/adapter.py` and `catalog/adapter.py`). You can find existing implementations in the `projects` directory. The default implementation provides basic usage for simple setups.

## ncml.py

`python ncml.py -h`

File paths read from stdin are examined and the results are stored in memory in a Pandas DataFrame. The columns of the DataFrame are a MultiIndex, where the first level values are the variables of the netCDF file and an additional 'GLOBALS' value which contains global attributes of the netCDF file. The DataFrame is grouped and for each group one NcML is created based on a [Jinja](https://jinja.palletsprojects.com/en/2.11.x/) template.

```bash
# Generate one NcML file using the default template
find /your/netcdf/files -type f | python ncml.py --dest test.ncml

# Generate multiple NcML files grouping by frequency
# This requires a global attribute 'frequency' in your netCDF files
find /your/netcdf/files -type f | python ncml.py --dest '{frequency}/test_{frequency}.ncml' --groupby frequency
```

Previous examples used the base template provided in `ncml/templates/base.ncml.j2`. You can provide your own template although it's required to store it in `ncml/templates`. In order to override this behaviour you have to provide your custom implmentation of `Adapter` in `ncml/adapter.py`.

```bash
cp ncml/templates/base.ncml.j2 ncml/templates/my-template.ncml.j2
find /your/netcdf/files -type f | python ncml.py --dest test.ncml --template my-template.ncml.j2
```

You can use `ncml.py` to store the DataFrame into the filesystem using `--save-dataframe FILE` (this functionality requires [PyTables](https://www.pytables.org/)). You can also use `--from-dataframe FILE` to load the DataFrame from FILE instead of providing file paths.

```bash
find /your/netcdf/files -type f | python ncml.py --save-dataframe test.hdf
```

To open the DataFrame from an interactive session:

```python
import pandas as pd

df = pd.read_hdf('test.hdf')
```

## catalog.py

`python catalog.py -h`

File paths (datasets) are read from stdin and from each one a dict is extracted, which contains the information about the dataset. The list of datasets is grouped and for each group a catalog is generated.

```bash
# Generate one catalog using the default template
find /your/datasets -type f | python catalog.py
find /your/datasets -type f | python catalog.py --dest alternative/directory
```

Previous examples used the base template provided in `catalog/templates/base.xml.j2`. You can provide your own template although it's required to store it in `catalog/templates`. In order to override this behaviour you have to provide your custom implmentation of `Adapter` in `catalog/adapter.py`.

```bash
cp catalog/templates/base.xml.j2 catalog/templates/my-template.xml.j2
find /your/netcdf/files -type f | python catalog.py --template my-template.xml.j2
```

To generate multiple catalogs, override the implementation of the group method in `catalog/adapter.py`. If `--root` is used, lines from stdin are processed as catalogs, not as datasets.
