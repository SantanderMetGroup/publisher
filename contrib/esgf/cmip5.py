#!/usr/bin/env python

import os
import sys
import re
import pandas as pd

import esgf

_help = '''Usage:
    cmip5.py [options] DATAFRAME

Options:
    -h, --help                      Show this message and exit.
    -d, --dest DESTINATION          Destination for HDF5 files (default is working directory).
    --variable-col VARIABLE         Column name where the variable is found (default is _DRS_variable).

    --group-time FACETS             Comma separated facets that group time periods of variables.
    --group-fx FACETS               Comma separated facets that, given a group grouped by --group-time, return it's fxs.
'''

group_latest_versions = (['_DRS_Dproject', '_DRS_Dproduct','_DRS_Dinstitution',
                          '_DRS_model', '_DRS_experiment', '_DRS_Dfrequency',
                          '_DRS_Drealm', '_DRS_Dtable', '_DRS_ensemble'])

ddrs = 'CMIP5/{_DRS_Dproduct}/{_DRS_Dinstitution}/{_DRS_model}/{_DRS_experiment}/{_DRS_Dfrequency}/{_DRS_Drealm}/{_DRS_Dtable}'
#fdrs = 'CMIP5_{_DRS_Dproduct}_{_DRS_Dinstitution}_{_DRS_model}_{_DRS_experiment}_{_DRS_Dfrequency}_{_DRS_Drealm}_{_DRS_Dtable}_{_DRS_ensemble}'
fdrs = 'CMIP5_{product}_{institute_id}_{model_id}_{experiment_id}_{frequency}_{modeling_realm}_{table_id_facet}_{_DRS_ensemble}'

def include_drs(df):
    default_file_drs = 'variable,table,model,experiment,ensemble,period'
    # cmip5 drs sucks (sometimes variable is part of drs sometimes it's not)
    v_pattern = r'v[0-9]{6}'
    version_last_directory = df[('GLOBALS', 'localpath')].str.split('/').str[-2].str.match(v_pattern)

    drs_df_1 = pd.DataFrame()
    drs_df_2 = pd.DataFrame()

    # when variable is the last directory in the DRS
    if not version_last_directory.all():
        directory_drs = 'Dproject,Dproduct,Dinstitution,Dmodel,Dexperiment,Drealm,Dfrequency,Dtable,Densemble,version,Dvariable'
        drs = ','.join([directory_drs, default_file_drs])
        drs_df_1 = esgf.get_drs_df(df[('GLOBALS', 'synda_localpath')], drs)
        drs_df_1.columns = [('GLOBALS', ''.join(['_DRS_', f])) for f in drs.split(',')]

    # when version is the last directory in the DRS
    if version_last_directory.any():
        directory_drs = 'Dproject,Dproduct,Dinstitution,Dmodel,Dexperiment,Drealm,Dfrequency,Dtable,Densemble,version'
        drs = ','.join([directory_drs, default_file_drs])
        drs_df_2 = esgf.get_drs_df(df[('GLOBALS', 'synda_localpath')], drs)
        drs_df_2.columns = [('GLOBALS', ''.join(['_DRS_', f])) for f in drs.split(',')]

    drs_df = pd.concat([drs_df_1, drs_df_2], ignore_index=True)
    df = pd.concat([df, drs_df], axis=1)

    return df

def arguments(argv):
    args = {
        'dest': os.path.join(os.getcwd(), '{_drs_filename}.hdf'),
        'variable_col': '_DRS_variable',
        'group_time': 'project_id,product,institute_id,model_id,experiment_id,frequency,modeling_realm,parent_experiment_rip',
        'group_fx': 'project_id,product,institute_id,model_id,experiment_id,modeling_realm',
        'dataframe': None,
    }

    arguments = len(argv) - 1
    position = 1
    while arguments >= position:
        if argv[position] == '-h' or argv[position] == '--help':
            print(_help)
            sys.exit(1)
        elif argv[position] == '-d' or argv[position] == '--dest':
            args['dest'] = argv[position+1]
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
        else:
            args['dataframe'] = argv[position]
            position+=1

    return args

if __name__ == '__main__':
    args = arguments(sys.argv)

    if args['dataframe'] is None:
        print(_help)
        sys.exit(1)

    df = pd.read_hdf(args['dataframe'], 'df')
    df[('GLOBALS', 'table_id_facet')] = df[('GLOBALS', 'table_id')].str.replace(r'\w+ (\w+) .*', '\\1')

    # fix time:units days since 0001-01
    subset = df[('time', 'units')] == "days since 0001-01"
    df.loc[subset, ('time', 'units')] = "days since 0001-01-01"

    if ('GLOBALS', '_DRS_period') in df.columns:
        periods = df[('GLOBALS', '_DRS_period')].str.split('-', expand=True)
        df[('GLOBALS', 'period1')] = periods.iloc[:,0].fillna(0).astype(int)
        df[('GLOBALS', 'period2')] = periods.iloc[:,1].fillna(0).astype(int)

    if ('GLOBALS', '_DRS_version') in df.columns:
        df[('GLOBALS', 'nversion')] = df[('GLOBALS', '_DRS_version')].str.replace('[a-zA-Z]', '').astype(int)
        df = esgf.get_latest_versions(df, group_latest_versions)

    subset = df[('GLOBALS', '_DRS_period')].isna()
    df = esgf.fix_time_values(df, args['group_time'].split(','), args['variable_col'])
    df.loc[~subset, ('time', '_values_str')] = df.loc[~subset, ('time', '_values')].apply(
            lambda times: ' '.join(times.astype(str)))

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
            synthetic_facet = '_synthetic_' + facet
            dataset[('GLOBALS', synthetic_facet)] = d[facet]

        D = dict(dataset[~dataset[('GLOBALS', args['variable_col'])].isin(esgf.vars_fx)]['GLOBALS'].iloc[0])
        #drs_directory = ddrs.format(**D)
        drs_filename = fdrs.format(**D).replace(" ", "-")
        #dataset.name = os.path.join(drs_directory, drs_filename)
        #dataset[('GLOBALS', '_drs')] = dataset.name
        #dataset[('GLOBALS', '_drs_directory')] = drs_directory
        dataset[('GLOBALS', '_drs_filename')] = drs_filename
        D = dict(dataset[~dataset[('GLOBALS', args['variable_col'])].isin(esgf.vars_fx)]['GLOBALS'].iloc[0])
        path = os.path.abspath(args['dest'].format(**D))

        path = esgf.render(dataset, path)
        print(path)
