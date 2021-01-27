#!/usr/bin/env python

import os, sys
import re
import numpy as np
import pandas as pd
import netCDF4

_help = '''Usage:
    todf [options] DATAFRAME

Options:
    -f, --file FILE             Read from FILE instead of stdin.
    -h, --help                  Display this message and exit.
    -g, --groupby GROUP         Comma separated names of columns to groupby under GLOBALS.

    --facets                    Comma separated facets for components of DRS.
    --facets-numeric            Comma separated facets from --facets that are numeric.
    --drs                       Regex to be compiled and searched for facets.
    --drs-prefix PREFIX         Prefix to be prepended to facets in --drs (default is '_DRS_').

    -v, --variables VARIABLE    Comma separated variables to read values from.
    --variables-column PREFIX   Name of variable column values (default is '_values').
'''

def read(files, variables, variable_column):
    for f in files:
        try:
            attrs = {}

            # f is a file in file system or a url
            if os.path.exists(f):
                attrs[('GLOBALS', 'size')] = os.stat(f).st_size
                attrs[('GLOBALS', 'localpath')] = os.path.abspath(f)
            else:
                attrs[('GLOBALS', 'localpath')] = f
        
            with netCDF4.Dataset(f) as ds:
                for attr in ds.ncattrs():
                    attrs[('GLOBALS', attr)] = ds.getncattr(attr)
            
                for variable in ds.variables:
                    dimensions = ds.variables[variable].dimensions
                    attrs[(variable, '_dimensions')] = ','.join(dimensions)
                    for attr in ds[variable].ncattrs():
                        attrs[(variable, attr)] = ds[variable].getncattr(attr)

                    # Read variable values
                    if variable in variables:
                        a = np.array(ds.variables[variable])
                        attrs[(variable, variable_column)] = a

                for dimension in ds.dimensions:
                    name = ds.dimensions[dimension].name
                    size = ds.dimensions[dimension].size
                    attrs[('_'.join(['_d', dimension]), 'name')] = name
                    attrs[('_'.join(['_d', dimension]), 'size')] = size
    
            yield attrs
        except Exception as err:
            print("Error while reading netCDF file {0}".format(f), file=sys.stderr)
            print("Error: {0}".format(err), file=sys.stderr)

def include_drs(df, drs, drs_facets, drs_prefix, facets_numeric):
    p = re.compile(drs)
    matches = df[('GLOBALS', 'localpath')].str.match(p)
    if not matches.all():
        print('Some input files do not match regex, exiting...', file=sys.stderr)
        sys.exit(1)

    drs_df = df[('GLOBALS', 'localpath')].str.extract(p)
    drs_df.columns = [('GLOBALS', ''.join([drs_prefix, f])) for f in drs_facets]

    # Convert numeric DRS columns from object to numeric
    if facets_numeric:
        facets_numeric = [('GLOBALS', ''.join([drs_prefix, f])) for f in facets_numeric]

        for f in facets_numeric:
            drs_df[f] = pd.to_numeric(drs_df[f])

    return pd.concat([df, drs_df], axis=1)

def args(argv):
    args = {
        'file': None,
        'dest': 'unnamed.hdf',
        'groupby': None,
        'drs': None,
        'facets': None,
        'facets_numeric': None,
        'drs_prefix': '_DRS_',
        'variables': [],
        'variables_column': '_values',
    }

    position = 1
    arguments = len(argv) - 1
    if arguments < 1:
        print(_help)
        sys.exit(1)

    while arguments >= position:
        if argv[position] == '-h' or argv[position] == '--help':
            print(_help)
            sys.exit(1)
        elif argv[position] == '-f' or argv[position] == '--file':
            args['file'] = argv[position+1]
            position+=2
        elif argv[position] == '-g' or argv[position] == '--groupby':
            fs = argv[position+1]
            args['groupby'] = [('GLOBALS', f) for f in fs.split(',')]
            position+=2
        elif argv[position] == '--drs':
            args['drs'] = argv[position+1]
            position+=2
        elif argv[position] == '--facets':
            args['facets'] = argv[position+1]
            position+=2
        elif argv[position] == '--facets-numeric':
            args['facets_numeric'] = argv[position+1]
            position+=2
        elif argv[position] == '--drs-prefix':
            args['drs_prefix'] = argv[position+1]
            position+=2
        elif argv[position] == '-v' or argv[position] == '--variables':
            args['variables'] = argv[position+1].split(',')
            position+=2
        elif argv[position] == '--variables-column':
            args['variables_column'] = argv[position+1]
            position+=2
        else:
            args['dest'] = argv[position]
            position+=1

    return args

if __name__ == '__main__':
    args = args(sys.argv)

    if args['file'] is None:
        df = pd.DataFrame(read(sys.stdin.read().splitlines(), args['variables'], args['variables_column']))
    else:
        inputs = open(args['file'], 'r')
        df = pd.DataFrame(read(inputs, args['variables'], args['variables_column']))
        inputs.close()

    if len(df) == 0:
        print('Empty DataFrame, exiting...', file=sys.stderr)
        sys.exit(1)

    df.columns = pd.MultiIndex.from_tuples(df.columns)

    if args['drs'] is not None:
        df = include_drs(df,
                         args['drs'],
                         args['facets'].split(','),
                         args['drs_prefix'],
                         args['facets_numeric'].split(','))

    if args['groupby'] is not None:
        for n,g in df.groupby(args['groupby']):
            D = dict(g.iloc[0]['GLOBALS'])
            dest = os.path.abspath(args['dest'].format(**D))
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with pd.HDFStore(dest) as store:
                store['df'] = g
            print(dest)
    else:
        D = dict(df.iloc[0]['GLOBALS'])
        dest = os.path.abspath(args['dest'].format(**D))
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        df.to_pickle(dest)
        print(dest)
