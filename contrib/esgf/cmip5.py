#!/usr/bin/env python

import os
import sys
import re
import pandas as pd

import esgf

_help = '''Usage:
    cmip5.py [-d DESTINATION] DATAFRAME

Options:
    -h, --help                          Show this message and exit.
    -d, --dest DESTINATION              Destination for HDF5 files (default is working directory).
'''

group_time = (['_DRS_Dproject', '_DRS_Dproduct','_DRS_Dinstitution', '_DRS_model',
               '_DRS_experiment', '_DRS_Dfrequency', '_DRS_Drealm',
               '_DRS_Dtable', '_DRS_ensemble'])
group_fx = ['_DRS_Dproject', '_DRS_Dproduct','_DRS_Dinstitution' ,'_DRS_model' ,'_DRS_experiment' , '_DRS_Drealm']
group_latest_versions = (['_DRS_Dproject', '_DRS_Dproduct','_DRS_Dinstitution',
                          '_DRS_model', '_DRS_experiment', '_DRS_Dfrequency',
                          '_DRS_Drealm', '_DRS_Dtable', '_DRS_ensemble'])

ddrs =  'CMIP5/{_DRS_Dproduct}/{_DRS_Dinstitution}/{_DRS_model}/{_DRS_experiment}/{_DRS_Dfrequency}/{_DRS_Drealm}/{_DRS_Dtable}'
fdrs = 'CMIP5_{_DRS_Dproduct}_{_DRS_Dinstitution}_{_DRS_model}_{_DRS_experiment}_{_DRS_Dfrequency}_{_DRS_Drealm}_{_DRS_Dtable}_{_DRS_ensemble}'

def clean(df):
    df[('GLOBALS', 'filename')] = df[('GLOBALS', 'localpath')].apply(lambda x: os.path.basename(x))
    df[('GLOBALS', 'nversion')] = df[('GLOBALS', '_DRS_version')].str.replace('[a-zA-Z]', '').astype(int)
    df[('GLOBALS', 'period1')] = df[('GLOBALS', '_DRS_period')].str.split('-', expand=True).iloc[:,0].fillna(0).astype(int)
    df[('GLOBALS', 'period2')] = df[('GLOBALS', '_DRS_period')].str.split('-', expand=True).iloc[:,1].fillna(0).astype(int)

    return df

def include_drs(df):
    # cmip5 drs sucks (sometimes variable is part of drs sometimes it's not)
    v_pattern = r'v[0-9]{6}'
    version_last_directory = df[('GLOBALS', 'localpath')].str.split('/').str[-2].str.match(v_pattern)

    # when variable is the last directory in the DRS
    if not version_last_directory.all():
        drs = 'Dproject,Dproduct,Dinstitution,Dmodel,Dexperiment,Drealm,Dfrequency,Dtable,Densemble,version,Dvariable,variable,table,model,experiment,ensemble,period'
        df = esgf.include_drs(df, drs)

    # when version is the last directory in the DRS
    if version_last_directory.any():
        drs = 'Dproject,Dproduct,Dinstitution,Dmodel,Dexperiment,Drealm,Dfrequency,Dtable,Densemble,version,variable,table,model,experiment,ensemble,period'
        df = esgf.include_drs(df, drs)

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
    df = include_drs(df)
    df = clean(df)
    df = esgf.get_latest_versions(df, group_latest_versions)
    df = esgf.get_time_values(df, group_latest_versions)

    # esgf.group requires 'variable'
    df[('GLOBALS', 'variable')] = df[('GLOBALS', '_DRS_variable')]
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
