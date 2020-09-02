#!/usr/bin/env python

import os, sys
import cfgrib
import pandas as pd
from multiprocessing import Pool

_help = '''Usage:
    gribtodf [options] DATAFRAME

Options:
    -f, --file FILE         Read from FILE instead of stdin.
    -h, --help              Display this message and exit.
    -j                      Number of parallel processes.
    -n, --name NAME         Name the dataframe with name NAME (default is '').
    -g, --groupby GROUP     Comma separated names of columns to groupby under GLOBALS.

    --drs DRS               Comma separated facets for components of DRS.
                            Note that the file extension is removed when processing the DRS.
    --drs-sep SEP           Regular expression separator for --drs (default is '[/_]',
                            that means split by '_' and '/').
    --drs-prefix PREFIX     Prefix to be prepended to facets in --drs (default is '_DRS').
'''

def read(f):
   try:
       attrs = {}
       attrs[('GLOBALS', 'size')] = os.stat(f).st_size
       attrs[('GLOBALS', 'localpath')] = os.path.abspath(f)

       with cfgrib.open_dataset(f, backend_kwargs={'indexpath': ''}) as ds:
           for attr in ds.attrs:
               attrs[('GLOBALS', attr)] = ds.attrs[attr]
           for variable in ds.variables:
               for attr in ds[variable].attrs:
                   attrs[(variable, attr)] = ds[variable].attrs[attr]

       return attrs
   except Exception as err:
       print("Error while reading GRIB file {0}".format(f), file=sys.stderr)
       print("Error: {0}".format(err), file=sys.stderr)

#def read(files):
#    for f in files:
#        try:
#            attrs = {}
#            attrs[('GLOBALS', 'size')] = os.stat(f).st_size
#            attrs[('GLOBALS', 'localpath')] = os.path.abspath(f)
#        
#            with cfgrib.open_dataset(f, backend_kwargs={'indexpath': ''}) as ds:
#                for attr in ds.attrs:
#                    attrs[('GLOBALS', attr)] = ds.attrs[attr]
#                for variable in ds.variables:
#                    for attr in ds[variable].attrs:
#                        attrs[(variable, attr)] = ds[variable].attrs[attr]
#        
#            yield attrs
#        except Exception as err:
#            print("Error while reading GRIB file {0}".format(f), file=sys.stderr)
#            print("Error: {0}".format(err), file=sys.stderr)

if __name__ == '__main__':
    args = {
        'file': None,
        'dest': 'unnamed.hdf',
        'name': '',
        'j': 1,
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
        elif sys.argv[position] == '-j':
            args['j'] = int(sys.argv[position+1])
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

    with Pool(args['j']) as pool:
        if args['file'] is None:
            d = pool.map(read, sys.stdin.read().splitlines())
        else:
            d = pool.map(read, args['file'].read().splitlines())
    df = pd.DataFrame(d)

#    if args['file'] is None:
#        df = pd.DataFrame(read(sys.stdin.read().splitlines()))
#    else:
#        df = pd.DataFrame(read(args['file'].read().splitlines()))

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
