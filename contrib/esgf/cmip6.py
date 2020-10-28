#!/usr/bin/env python

import os
import sys
import re
import pandas as pd
import netCDF4

import esgf

_help = '''Usage:
    cmip6.py [OPTIONS] DATAFRAME

Options:
    -h, --help                      Show this message and exit.
    -d, --dest DESTINATION          Destination for HDF5 files (default is working directory).

    --variable-col VARIABLE         Column name where the variable is found (default is _DRS_variable).
    --latest VERSION_COLUMN         Column name to identify latest versions.

    --grid-label-col COLUMN         Column that identifies grid_label.
    --filter-grid-labels FACETS     Comma separated facets that group when filtering grid labels.

    --group-time FACETS             Comma separated facets that group time periods of variables.
    --group-fx FACETS               Comma separated facets that, given a group grouped by --group-time, return it's fxs.
'''

def filter_grid_labels(df, grid_label, facets):
    def gridlabel_to_int(label):
        if label == "gn":
            return 0
        elif label == "gr":
            return 1
        else:
            # priority gn > gr > gr1 > gr2 > ...., 0 is greatest priority
            return int(re.sub("[^0-9]", "", label)) + 1

    df[('GLOBALS', 'ngrid_label')] = df[('GLOBALS', grid_label)].apply(gridlabel_to_int)
    unique_grid_labels = []
    how_to_group = [('GLOBALS', f) for f in facets.split(',')]

    for _,group in df.groupby(how_to_group):
        unique_grid_labels.append(group.nlargest(1, ('GLOBALS', 'ngrid_label'), keep='all'))

    return pd.concat(unique_grid_labels)

def clean(df):
    df[('GLOBALS', 'filename')] = df[('GLOBALS', 'localpath')].apply(lambda x: os.path.basename(x))

    # KACE-1-0-G monthly datasets have got 'frequency=day' in global attributes
    kace_1_0_g_mon = ((df[('GLOBALS', '_DRS_model')] == 'KACE-1-0-G') & (df[('GLOBALS', '_DRS_table')] == 'Amon'))
    df.loc[kace_1_0_g_mon, ('GLOBALS', 'frequency')] = 'mon'

    return df

def parse_args(argv):
    args = {
        'dest': os.path.join(os.getcwd(), 'unnamed.hdf'),
        'dataframe': None,
        'variable_col': '_DRS_variable',
        'latest': None,
        'group_time': 'mip_era,activity_id,institution_id,model_id,experiment_id,variant_label,table_id,grid_label',
        'group_fx': 'mip_era,activity_id,institution_id,model_id,experiment_id,variant_label,grid_label',
        'grid_label_col': 'grid_label',
        'filter_grid_labels': None,
    }

    arguments = len(sys.argv) - 1
    position = 1
    while arguments >= position:
        if sys.argv[position] == '-h' or sys.argv[position] == '--help':
            print(_help)
            sys.exit(1)
        elif sys.argv[position] == '-d' or sys.argv[position] == '--dest':
            args['dest'] = sys.argv[position+1]
            position+=2
        elif argv[position] == '--variable-col':
            args['variable_col'] = argv[position+1]
            position+=2
        elif argv[position] == '--group-time':
            args['group_time'] = argv[position+1]
            position+=2
        elif argv[position] == '--group-fx':
            args['group_fx'] = argv[position+1]
            position+=2
        elif argv[position] == '--latest':
            args['latest'] = argv[position+1]
            position+=2
        elif argv[position] == '--filter-grid-labels':
            args['filter_grid_labels'] = argv[position+1]
            position+=2
        elif argv[position] == '--grid-label-col':
            args['grid_label_col'] = argv[position+1]
            position+=2
        else:
            args['dataframe'] = sys.argv[position]
            position+=1

    return args

if __name__ == '__main__':
    args = parse_args(sys.argv)

    if args['dataframe'] is None:
        print(_help)
        sys.exit(1)

    df = pd.read_hdf(args['dataframe'], 'df')
    df = clean(df)
    # In cordex and cmip5 frequency is part of drs but not in cmip6
    #df[('GLOBALS', '_DRS_Dfrequency')] = df[('GLOBALS', 'frequency')]

    if args['filter_grid_labels'] is not None:
        df = filter_grid_labels(df, args['grid_label_col'], args['filter_grid_labels'])

    if args['latest'] is not None:
        df = esgf.get_latest_versions(df, args['group_time'])

    # Report missing files in time series
    #esgf.test_missing_nc(df[df[('GLOBALS', '_DRS_Dfrequency')] != 'fx'], group_latest_versions)

    # Set same calendar for time values
    df = esgf.fix_time_values(df, args['group_time'], args['variable_col'])

    how_to_group = [('GLOBALS', facet) for facet in args['group_time'].split(',')]
    time_groups = df[~df[('GLOBALS', args['variable_col'])].isin(esgf.vars_fx)].groupby(how_to_group)
    for name, group in time_groups:
        # include corresponding fx variables
        d = dict(zip(args['group_time'].split(','), name))
        filter_dict = {k: d[k] for k in args['group_fx'].split(',')} 
        all_fxs = df[df[('GLOBALS', args['variable_col'])].isin(esgf.vars_fx)]
        group_fxs = all_fxs.loc[(df['GLOBALS'][filter_dict.keys()] == pd.Series(filter_dict)).all(axis=1)]

        # this would be the full dataset
        dataset = (pd.concat([group, group_fxs])
                     .sort_values(by=[('GLOBALS', args['variable_col']), ('GLOBALS', 'localpath')])
                     .reset_index(drop=True))

        # "synthetic" columns: substitute fx's facets (eg: ensemble=r0i0p0, frequency=fx, ...) by it's time value
        list_time = args['group_time'].split(',')
        list_fx = args['group_fx'].split(',')
        l = [facet for facet in list_time if facet not in list_fx]
        for facet in l:
            synthetic_facet = '_synthetic' + facet
            dataset[('GLOBALS', synthetic_facet)] = d[facet]

        D = dict(dataset[~dataset[('GLOBALS', args['variable_col'])].isin(esgf.vars_fx)]['GLOBALS'].iloc[0])
        path = os.path.abspath(args['dest'].format(**D))

        path = esgf.render(dataset, path)
        print(path)
