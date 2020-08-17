#!/usr/bin/env python

import os
import sys
import pandas as pd

import esgf

_help = '''Usage:
    cordex.py [-d DESTINATION] DATAFRAME

Options:
    -h, --help                          Show this message and exit.
    -d, --dest DESTINATION              Destination for HDF5 files (default is working directory).
'''

group_time = (['_DRS_Dproject', '_DRS_Dproduct', '_DRS_domain', '_DRS_Dinstitution',
               '_DRS_model', '_DRS_experiment', '_DRS_ensemble',
               '_DRS_rcm', '_DRS_rcm_version', '_DRS_Dfrequency'])
group_fx = (['_DRS_Dproject', '_DRS_Dproduct', '_DRS_domain', '_DRS_Dinstitution',
             '_DRS_model', '_DRS_experiment', '_DRS_rcm', '_DRS_rcm_version'])
group_latest_versions = (['_DRS_Dproject', '_DRS_Dproduct', '_DRS_domain', '_DRS_Dinstitution',
                          '_DRS_model', '_DRS_experiment', '_DRS_ensemble',
                          '_DRS_rcm', '_DRS_rcm_version', '_DRS_Dfrequency', '_DRS_variable'])

drs = 'Dproject,Dproduct,Ddomain,Dinstitution,Dmodel,Dexperiment,Densemble,Drcm,Drcm_version,Dfrequency,Dvariable,version,variable,domain,model,experiment,ensemble,rcm,rcm_version,frequency,period'

ddrs = 'cordex/{_DRS_Dproduct}/{_DRS_domain}/{_DRS_Dinstitution}/{_DRS_model}/{_DRS_experiment}/{_DRS_rcm}/{_DRS_rcm_version}/{_DRS_Dfrequency}'
fdrs = 'CORDEX_{_DRS_Dproduct}_{_DRS_domain}_{_DRS_model}_{_DRS_experiment}_{_DRS_ensemble}_{_DRS_rcm}_{_DRS_rcm_version}_{_DRS_Dfrequency}'

def clean(df):
    df[('GLOBALS', 'filename')] = df[('GLOBALS', 'localpath')].apply(lambda x: os.path.basename(x))
    df[('GLOBALS', 'nversion')] = df[('GLOBALS', '_DRS_version')].str.replace('[a-zA-Z]', '').astype(int)

    # netcdfs from CAS-22_NCC-NorESM1-M_rcp26_r0i0p0_GERICS-REMO2015 have ensemble before extension
    # set every _DRS_period column of fixed variables to None to prevent more failures
    df.loc[df[('GLOBALS', 'frequency')] == 'fx', ('GLOBALS', '_DRS_period')] = None
    df[('GLOBALS', 'period1')] = df[('GLOBALS', '_DRS_period')].str.split('-', expand=True).iloc[:,0].fillna(0).astype(int)
    df[('GLOBALS', 'period2')] = df[('GLOBALS', '_DRS_period')].str.split('-', expand=True).iloc[:,1].fillna(0).astype(int)

    df[('time', 'units')] = df[('time', 'units')].str.replace(' UTC', '')

    # Add institute to RCMModelName (institute-rcm)
    for r in df.index:
        if df.loc[r, ('GLOBALS', '_DRS_Dinstitution')] not in df.loc[r, ('GLOBALS', '_DRS_rcm')]:
            df.loc[r, ('GLOBALS', '_DRS_rcm')] = \
                '-'.join([df.loc[r, ('GLOBALS', '_DRS_Dinstitution')], df.loc[r, ('GLOBALS', '_DRS_rcm')]])

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
    df = esgf.get_latest_versions(df, group_latest_versions)
    df = esgf.get_time_values(df, group_latest_versions)

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
