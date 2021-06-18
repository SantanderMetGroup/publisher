#!/usr/bin/env python

import os, sys
import re
import numpy as np
import pandas as pd
import netCDF4

_help = '''Usage:
    todf [options] DATAFRAME

Options:
    -h, --help                  Display this message and exit.

    -f, --file FILE             Read from FILE instead of stdin.
    -g, --groupby GROUP         Comma separated names of columns to groupby under GLOBALS.
    --numeric COLS              Comma separated columns under GLOBALS that are numeric.
    --drs DRS                   Regex to be compiled and searched for facets.

    --cols COLUMNS              Column names split by ','.
    --separator SEPARATOR       Column separator, default is ','.

    -v, --variables VARIABLE    Comma separated variables to read values from.
    --values-col NAME           Name of variable column values (default is '_values').
'''

def read(f, variables, values_column):
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
                    attrs[(variable, values_column)] = a

            for dimension in ds.dimensions:
                name = ds.dimensions[dimension].name
                size = ds.dimensions[dimension].size
                attrs[('_'.join(['_d', dimension]), 'name')] = name
                attrs[('_'.join(['_d', dimension]), 'size')] = size

        return attrs
    except Exception as err:
        print("Error while reading netCDF file {0}".format(f), file=sys.stderr)
        print("Error: {0}".format(err), file=sys.stderr)

def include_drs(df, drs):
    p = re.compile(drs)
    matches = df[('GLOBALS', 'localpath')].str.match(p)
    if not matches.all():
        print('Some input files do not match regex, exiting...', file=sys.stderr)
        sys.exit(1)

    drs_df = df[('GLOBALS', 'localpath')].str.extract(p)
    drs_df.columns = [('GLOBALS', f) for f in drs_df.columns]

    return pd.concat([df, drs_df], axis=1)

def args(argv):
    args = {
        'file': None,
        'dest': 'unnamed.pickle',
        'groupby': None,
        'drs': None,
        'numeric': None,
        'variables': [],
        'values_col': '_values',

        # Columns
        'cols': ['localpath'],
        'separator': ',',
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
        elif argv[position] == '--numeric':
            args['numeric'] = argv[position+1].split(',')
            position+=2
        elif argv[position] == '-v' or argv[position] == '--variables':
            args['variables'] = argv[position+1].split(',')
            position+=2
        elif argv[position] == '--variables-column':
            args['values_col'] = argv[position+1]
            position+=2

        # Columns
        elif argv[position] == '--cols':
            args['cols'] = argv[position+1].split(',')
            position+=2
        elif argv[position] == '--separator':
            args['separator'] = argv[position+1]
            position+=2

        # Destination file
        else:
            args['dest'] = argv[position]
            position+=1

    return args

class InputReader(object):
    def __init__(self, cols, sep):
        self.cols = cols
        self.sep = sep

    def read(self, variables, values_col):
        raise NotImplementedError

    def process(self, line, variables, values_col):
        parts = line.split(self.sep)
        attrs = {}
        attrs.update( read(parts[0].rstrip('\n'), variables, values_col) )
        attrs.update( {('GLOBALS', k): v for k,v in zip(self.cols,parts)} )

        return attrs

class StdinInputReader(InputReader):
    def __init__(self, cols, sep):
        super().__init__(cols, sep)
        self.frm = sys.stdin

    def read(self, variables, values_col):
        for line in self.frm:
            yield self.process(line, variables, values_col)

class FileInputReader(InputReader):
    def __init__(self, cols, sep, frm):
        super().__init__(cols, sep)
        self.frm = open(frm, 'r')

    def read(self, variables, values_col):
        for line in self.frm:
            yield self.process(line, variables, values_col)
        self.frm.close()

if __name__ == '__main__':
    args = args(sys.argv)

    if args['file']:
        reader = FileInputReader(args['cols'], args['separator'], args['file'])
    else:
        reader = StdinInputReader(args['cols'], args['separator'])

    df = pd.DataFrame(reader.read(args['variables'], args['values_col']))

    if len(df) == 0:
        print('Empty DataFrame, exiting...', file=sys.stderr)
        sys.exit(1)

    df.columns = pd.MultiIndex.from_tuples(df.columns)

    if args['drs'] is not None:
        df = include_drs(df, args['drs'])

    if args['numeric'] is not None:
        numeric = [('GLOBALS', f) for f in args['numeric']]
        for f in numeric:
            df[f] = pd.to_numeric(df[f])

    if args['groupby'] is not None:
        for n,g in df.groupby(args['groupby']):
            D = dict(g.iloc[0]['GLOBALS'])
            dest = os.path.abspath(args['dest'].format(**D))
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            g.to_pickle(dest)
            print(dest)
    else:
        D = dict(df.iloc[0]['GLOBALS'])
        dest = os.path.abspath(args['dest'].format(**D))
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        df.to_pickle(dest)
        print(dest)
