#!/usr/bin/env python

import os, sys
import pandas as pd
import netCDF4

_help = '''Usage:
    todf [options] DATAFRAME

Options:
    -f, --file FILE         Read from FILE instead of stdin.
    -h, --help              Display this message and exit.
    -n, --name NAME         Name the dataframe with name NAME (default is '').
    -g, --groupby GROUP     Comma separated names of columns to groupby under GLOBALS.

    --drs DRS               Comma separated facets for components of DRS.
                            Note that the file extension is removed when processing the DRS.
    --drs-sep SEP           Regular expression separator for --drs (default is '[/_]',
                            that means split by '_' and '/').
    --drs-prefix PREFIX     Prefix to be prepended to facets in --drs (default is '_DRS').
'''

def read(files):
    for f in files:
        try:
            attrs = {}
            attrs[('GLOBALS', 'size')] = os.stat(f).st_size
            attrs[('GLOBALS', 'localpath')] = os.path.abspath(f)
        
            with netCDF4.Dataset(f) as ds:
                for attr in ds.ncattrs():
                    attrs[('GLOBALS', attr)] = ds.getncattr(attr)
            
                for variable in ds.variables:
                    for attr in ds[variable].ncattrs():
                        attrs[(variable, attr)] = ds[variable].getncattr(attr)
    
                # This is so often required that it's fair to include it here
                # Just be careful about overwriting existing attributes
                if 'time' in ds.variables:
                    attrs[('time', 'ncoords')] = ds.variables['time'].size
                    attrs[('time', 'value0')] = ds.variables['time'][0]
                    attrs[('time', 'increment')] = ds.variables['time'][1] - ds.variables['time'][0]
        
            yield attrs
        except Exception as err:
            print("Error while reading netCDF file {0}".format(f), file=sys.stderr)
            print("Error: {0}".format(err), file=sys.stderr)

if __name__ == '__main__':
    args = {
        'file': None,
        'dest': 'unnamed.hdf',
        'name': '',
        'groupby': None,
        'drs': None,
        'drs_sep': '[/_]',
    }

    position = 1
    arguments = len(sys.argv) - 1
    if arguments < 1:
        print(_help)
        sys.exit(1)

    while arguments >= position:
        if sys.argv[position] == '-h' or sys.argv[position] == '--help':
            print(_help)
            sys.exit(1)
        elif sys.argv[position] == '-f' or sys.argv[position] == '--file':
            args['file'] = sys.argv[position+1]
            position+=2
        elif sys.argv[position] == '-n' or sys.argv[position] == '--name':
            args['name'] = sys.argv[position+1]
            position+=2
        elif sys.argv[position] == '-g' or sys.argv[position] == '--groupby':
            fs = sys.argv[position+1]
            args['groupby'] = [('GLOBALS', f) for f in fs.split(',')]
            position+=2
        elif sys.argv[position] == '--drs':
            args['drs'] = sys.argv[position+1]
            position+=2
        elif sys.argv[position] == '--drs-sep':
            args['drs_sep'] = sys.argv[position+1]
            position+=2
        elif sys.argv[position] == '--drs-prefix':
            args['drs_sep'] = sys.argv[position+1]
            position+=2
        else:
            args['dest'] = sys.argv[position]
            position+=1

    if args['file'] is None:
        df = pd.DataFrame(read(sys.stdin.read().splitlines()))
    else:
        df = pd.DataFrame(read(args['file'].read().splitlines()))

    if len(df) == 0:
        print('Empty DataFrame, exiting...')
        sys.exit(1)

    df.columns = pd.MultiIndex.from_tuples(df.columns)
    df.name = args['name']

    if args['drs'] is not None:
        drs = args['drs'].split(',')
        ndrs = len(drs)
        localpaths = df[('GLOBALS', 'localpath')].apply(lambda x: os.path.splitext(x)[0])
        drs_df = localpaths.str.split(args['drs_sep'], expand=True).iloc[:, -ndrs:]
        drs_df.columns = [('GLOBALS', '_'.join(['_DRS', f])) for f in drs]
        df = pd.concat([df, drs_df], axis=1)

# https://github.com/pandas-dev/pandas/issues/31199
#    # transform object columns to string
#    object_columns = df.select_dtypes(include=['object']).columns
#    df[object_columns] = df[object_columns].astype('string')
    import warnings
    warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)


    if args['groupby'] is not None:
        for n,g in df.groupby(args['groupby']):
            D = dict(g.iloc[0]['GLOBALS'])
            dest = args['dest'].format(**D)
            with pd.HDFStore(dest) as store:
                store['df'] = g
            print(dest)
    else:
        D = dict(df.iloc[0]['GLOBALS'])
        dest = args['dest'].format(**D)
        with pd.HDFStore(dest) as store:
            store['df'] = df
        print(dest)
