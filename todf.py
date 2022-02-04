#!/usr/bin/env python

import os, sys
import re
import numpy as np
import pandas as pd
import netCDF4

def help():
    print('''Usage:
    todf [options] DATAFRAME

    Options:

    -h, --help                    Display this message and exit.

    --drs DRS                     Regex to be compiled and searched for facets.
    -v, --variables VARIABLE      Comma separated variables to read values from.
    --values-by-attr NAME VALUE   Same as --variables but this matches by the attribute NAME with value VALUE.''', file=sys.stderr)

def args(argv):
    args = {
        'dest': 'unnamed.pickle',
        "format": None,
        'drs': None,
        'variables': [],
        'values_by_attr': [],
    }

    position = 1
    arguments = len(argv) - 1
    if arguments < 1:
        help()
        sys.exit(1)

    while arguments >= position:
        if argv[position] == '-h' or argv[position] == '--help':
            help()
            sys.exit(1)
        elif argv[position] == '--format':
            args['format'] = argv[position+1]
            position+=2
        elif argv[position] == '--drs':
            args['drs'] = argv[position+1]
            position+=2
        elif argv[position] == '-v' or argv[position] == '--variables':
            args['variables'] = argv[position+1].split(',')
            position+=2
        elif argv[position] == '--values-by-attr':
            name = argv[position+1]
            value = argv[position+2]
            args['values_by_attr'].append((name,value))
            position+=3
        else:
            args['dest'] = argv[position]
            position+=1

    return args

class Reader:
    def read(self, values, drs):
        for line in sys.stdin:
            fname = line.rstrip("\n")
            for row in self.read_nc(fname, values, drs):
                yield row

    def read_nc(self, f, values, drs):
        if values is None:
            values = list()

        yield [f, None, None, None, "size", int(os.stat(f).st_size), None, None]
        with netCDF4.Dataset(f) as ds:
            if drs is not None:
                p = re.compile(drs)
                matches = p.search(f)
                for match in matches.groupdict():
                    yield [f, match, matches.groupdict()[match], None, None, None, None, None]

            for dimension in ds.dimensions:
                yield [f, None, None, None, None, None, dimension, ds.dimensions[dimension].size]

            for variable in ds.variables:
                yield [f, None, None, variable, None, None, None, None]

                for dimension in ds.variables[variable].dimensions:
                    yield [f, None, None, variable, None, None, dimension, ds.dimensions[dimension].size]

                if variable in values:
                    yield [f, None, None, variable, "__values__", np.array(ds.variables[variable]), None, None]

class FullReader(Reader):
    def read_nc(self, f, values, drs):
        if values is None:
            values = list()

        for row in super().read_nc(f, values, drs):
            yield row

        with netCDF4.Dataset(f) as ds:
            for attr in ds.ncattrs():
                yield [f, None, None, None, attr, ds.getncattr(attr), None, None]

            for variable in ds.variables:
                for attr in ds[variable].ncattrs():
                    yield [f, None, None, variable, attr, ds[variable].getncattr(attr), None, None]

if __name__ == '__main__':
    args = args(sys.argv)

    if args["format"] == "no_attrs":
        reader = Reader()
    else:
        reader = FullReader()

    columns = ["filename", "facet", "facet_value", "variable", "attr", "attr_value", "dimension", "dimension_size"]
    df = pd.DataFrame(reader.read(args["variables"], args["drs"]), columns=columns)

    if len(df) == 0:
        print('Empty DataFrame, exiting...', file=sys.stderr)
        sys.exit(1)

    facets = df[~df["facet"].isna()][["facet", "facet_value"]].drop_duplicates("facet")
    facets = dict(zip(facets["facet"], facets["facet_value"]))

    df.to_pickle(args["dest"].format(**facets))