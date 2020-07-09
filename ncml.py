#!/usr/bin/env python

import os, sys
import pandas as pd

from ncml.adapter import Adapter
from ncml.adapter import NetcdfAdapter

from projects.esgf.cmip6 import Cmip6NcmlAdapter
from projects.esgf.cmip5 import Cmip5NcmlAdapter
from projects.esgf.cordex import CordexNcmlAdapter

from projects.cordexEsdm.cordexEsdm import EcearthCordexEsdmNcmlAdapter, InterimCordexEsdmNcmlAdapter

from projects.circulationTypes.circulation_types import CirculationTypesCmip6NcmlAdapter
from projects.circulationTypes.circulation_types import CirculationTypesCmip5NcmlAdapter

_help = '''Usage:
    python ncml.py [--adapter ADAPTER] [--save-dataframe FILE] [--from-dataframe FILE]

Options:
    -h, --help                  Show this message.
    --adapter ADAPTER           Use ADAPTER instead of NetcdfAdapter from 'ncml/adapter.py'
    --save-dataframe FILE       After parsing files from stdin, save the dataframe to FILE and exit.
    --from-dataframe FILE       Instead of reading from stdin, load dataframe from FILE.

Base Adapter options:
    --dest FILE                 Save ncml to FILE.
    --groupby EXPR              Comma separated values of global attributes to group elements of dataframe.
    --template FILE             Use template from 'ncml/templates' directory (default='base.ncml.j2').
'''

def get_data(adapter, files):
    for f in files:
        try:
            metadata = adapter.read(f)
            yield {(outerKey, innerKey): value for outerKey, innerDict in metadata.items() for innerKey, value in innerDict.items()}
        except Exception as e:
            print('{},MetadataReadException,{}'.format(f, e), file=sys.stderr)

def create_dataframe(adapter):
    files = sys.stdin.read().splitlines()
    if len(files) == 0:
        sys.exit(1)

    df = pd.DataFrame(get_data(adapter, files))
    # If df is empty: TypeError: Cannot infer number of levels from empty list (see above if test)
    df.columns = pd.MultiIndex.from_tuples(df.columns)

    return df

if __name__ == '__main__':
    # Parse arguments
    adapter, save_dataframe, from_dataframe = None, None, None
    adapter_opts = {}
    position = 1
    arguments = len(sys.argv) - 1
    while arguments >= position:
        if sys.argv[position] == '-h' or sys.argv[position] == '--help':
            print(_help)
            sys.exit(0)
        elif sys.argv[position] == '--adapter':
            adapter = sys.argv[position+1]
        elif sys.argv[position] == '--save-dataframe':
            save_dataframe = sys.argv[position+1]
        elif sys.argv[position] == '--from-dataframe':
            from_dataframe = sys.argv[position+1]
        else: # Adapter arguments
            opt = sys.argv[position].lstrip('-')
            adapter_opts[opt] = sys.argv[position+1]
        position+=2

    if adapter is None:
        adapter = NetcdfAdapter(**adapter_opts)
    else:
        adapter = globals()[adapter](**adapter_opts)

    if from_dataframe:
        df = pd.read_hdf(from_dataframe)
    else:
        df = create_dataframe(adapter)

    if save_dataframe:
        store = pd.HDFStore(save_dataframe)
        store['df'] = df
        sys.exit(0)

    preprocessed = adapter.preprocess(df)
    # Each loop iteration generates a NcML
    for group in adapter.group(preprocessed):
        path = adapter.to_ncml(group)
        adapter.test(group, path)
        print(path)
