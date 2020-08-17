#!/usr/bin/env python

import os
import sys
import re
import pandas as pd
import netCDF4

import esgf

_help = '''Usage:
    cmip6.py [-d DESTINATION] DATAFRAME

Options:
    -h, --help                          Show this message and exit.
    -d, --dest DESTINATION              Destination for HDF5 files (default is working directory).
'''

group_time = (['_DRS_Dproject', '_DRS_Dproduct','_DRS_model' ,'_DRS_Dinstitution' ,
               '_DRS_experiment' ,'_DRS_ensemble' , '_DRS_table', '_DRS_grid_label'])
group_fx = (['_DRS_Dproject', '_DRS_Dproduct','_DRS_model' ,'_DRS_Dinstitution' ,
             '_DRS_experiment' ,'_DRS_ensemble'])
group_latest_versions = (['_DRS_Dproject', '_DRS_Dproduct','_DRS_model' ,'_DRS_Dinstitution',
                          '_DRS_experiment' ,'_DRS_ensemble' , '_DRS_table',
                          '_DRS_grid_label', '_DRS_variable'])

drs='Dproject,Dproduct,Dinstitution,Dmodel,Dexperiment,Densemble,Dtable,Dvariable,Dgrid_label,version,variable,table,model,experiment,ensemble,grid_label,period'

ddrs =  'CMIP6/{_DRS_Dproduct}/{_DRS_Dinstitution}/{_DRS_model}/{_DRS_experiment}/{_DRS_table}'
fdrs = 'CMIP6_{_DRS_Dproduct}_{_DRS_Dinstitution}_{_DRS_model}_{_DRS_experiment}_{_DRS_ensemble}_{_DRS_table}'

def filter_grid_labels(df):
    def gridlabel_to_int(grid_label):
        if grid_label == "gn":
            return 0
        elif grid_label == "gr":
            return 1
        else:
            # priority gn > gr > gr1 > gr2 > ...., 0 is greatest priority
            return int(re.sub("[^0-9]", "", grid_label)) + 1

    df[('GLOBALS', 'ngrid_label')] = df[('GLOBALS', '_DRS_grid_label')].apply(gridlabel_to_int)
    unique_grid_labels = []
    facets = (['_DRS_Dproject', '_DRS_Dproduct', '_DRS_model', '_DRS_Dinstitution',
               '_DRS_experiment', '_DRS_ensemble' , '_DRS_table'])
    how_to_group = [('GLOBALS', f) for f in facets]

    for _,group in df.groupby(how_to_group):
        unique_grid_labels.append(group.nlargest(1, ('GLOBALS', 'ngrid_label'), keep='all'))

    return pd.concat(unique_grid_labels)

def clean(df):
    df[('GLOBALS', 'filename')] = df[('GLOBALS', 'localpath')].apply(lambda x: os.path.basename(x))
    df[('GLOBALS', 'nversion')] = df[('GLOBALS', '_DRS_version')].str.replace('[a-zA-Z]', '').astype(int)
    df[('GLOBALS', 'period1')] = df[('GLOBALS', '_DRS_period')].str.split('-', expand=True).iloc[:,0].fillna(0).astype(int)
    df[('GLOBALS', 'period2')] = df[('GLOBALS', '_DRS_period')].str.split('-', expand=True).iloc[:,1].fillna(0).astype(int)

    # KACE-1-0-G monthly datasets have got 'frequency=day' in global attributes
    kace_1_0_g_mon = ((df[('GLOBALS', '_DRS_model')] == 'KACE-1-0-G') & (df[('GLOBALS', '_DRS_table')] == 'Amon'))
    df.loc[kace_1_0_g_mon, ('GLOBALS', 'frequency')] = 'mon'

    return df

if __name__ == '__main__':
    args = {
        'dest': os.path.join(os.getcwd(), '{_drs_filename}.hdf'),
        'dataframe': None,
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
        else:
            args['dataframe'] = sys.argv[position]
            position+=1

    if args['dataframe'] is None:
        print(_help)
        sys.exit(1)

    df = pd.read_hdf(args['dataframe'], 'df')
    df = esgf.include_drs(df, drs)
    df = clean(df)
    # In cordex and cmip5 frequency is part of drs but not in cmip6
    df[('GLOBALS', '_DRS_Dfrequency')] = df[('GLOBALS', 'frequency')]
    df = filter_grid_labels(df)
    df = esgf.get_latest_versions(df, group_latest_versions)
    df = esgf.get_time_values(df, group_latest_versions)

    # Report missing files in time series
    esgf.test_missing_nc(df[df[('GLOBALS', '_DRS_Dfrequency')] != 'fx'], group_latest_versions)

    for dataset in esgf.group(df, group_time, group_fx):
        D = dict(dataset[dataset[('GLOBALS', '_DRS_Dfrequency')] != 'fx']['GLOBALS'].iloc[0])
        drs_directory = ddrs.format(**D)
        drs_filename = fdrs.format(**D)
        dataset.name = os.path.join(drs_directory, drs_filename)
        dataset[('GLOBALS', '_drs')] = dataset.name
        dataset[('GLOBALS', '_drs_directory')] = drs_directory
        dataset[('GLOBALS', '_drs_filename')] = drs_filename
        D = dict(dataset[dataset[('GLOBALS', '_DRS_Dfrequency')] != 'fx']['GLOBALS'].iloc[0])
        path = os.path.abspath(args['dest'].format(**D))

        esgf.render(dataset, path)
