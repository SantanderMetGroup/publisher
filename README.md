# SantanderMetGroup/publisher

Tool for generation of datasets and catalogs.

## todf.py

`python todf.py -h`

`todf.py` stands for To DataFrame. It reads file paths ([netCDF](https://www.unidata.ucar.edu/software/netcdf/) at the moment) from stdin and generates [Pandas](https://pandas.pydata.org/) multicolumn DataFrame stored in Pickle format.

```bash
find /your/netcdf/files -type f | python todf.py test.pickle
```

You can inspect the results in an interactive session:

```python
import pandas as pd

# You will find that columns are a Multiindex where the first level
# are netCDF variables and global attributes and second level are
# attributes. Each row represents a file and it's attribute values.
df = pd.read_pickle('test.pickle')
```

## jdataset.py

`python jdataset.py -h`

`jdataset.py` stands for [Jinja](https://jinja.palletsprojects.com/en/2.11.x/) dataset. It reads [Pandas](https://pandas.pydata.org/) DataFrame via Pickle and generates textual datasets (ex. [NcML](https://www.unidata.ucar.edu/software/netcdf-java/v4.6/ncml/Tutorial.html)).

```bash
# This uses the template 'base.ncml.j2' in templates
python jdataset.py test.pickle

# If your netCDFs have got a global attribute called 'variable' you can try
python jdataset.py test.pickle --groupby 'variable' --dest '{variable}.ncml'
```

## contrib/esgf

Scripts used in the generation of NcMLs for data coming from [ESGF](http://esgf.llnl.gov/).
