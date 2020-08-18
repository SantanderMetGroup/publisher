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

#variables = (['aclwdnt', 'alb', 'areacella', 'areacellr', 'clfr1000',
#'clfr200', 'clfr300', 'clfr400', 'clfr500', 'clfr600', 'clfr700', 'clfr850',
#'clfr875', 'clfr900', 'clfr925', 'clfr950', 'clfr975', 'clh', 'clice1000',
#'clice200', 'clice300', 'clice400', 'clice500', 'clice600', 'clice700',
#'clice850', 'clice875', 'clice900', 'clice925', 'clice950', 'clice975',
#'clivi', 'cll', 'clm', 'clt', 'clwmr1000', 'clwmr200', 'clwmr300', 'clwmr400',
#'clwmr500', 'clwmr600', 'clwmr700', 'clwmr850', 'clwmr875', 'clwmr900',
#'clwmr925', 'clwmr950', 'clwmr975', 'clwvi', 'evspsbl', 'evspsblpot', 'hfls',
#'hfss', 'hufs', 'hur1000', 'hur200', 'hur300', 'hur400', 'hur500', 'hur600',
#'hur700', 'hur850', 'hur875', 'hur900', 'hur925', 'hur950', 'hur975', 'hurs',
#'hus1000', 'hus200', 'hus300', 'hus400', 'hus500', 'hus600', 'hus700',
#'hus850', 'hus875', 'hus900', 'hus925', 'hus950', 'hus975', 'huss', 'mrfso',
#'mross', 'mrro', 'mrros', 'mrso', 'mrsofc', 'mrsos', 'mrsosat', 'mrsosd',
#'mrsowp', 'orog', 'pr', 'prc', 'prhmax', 'prls', 'prsn', 'prw', 'ps', 'psl',
#'rlds', 'rlus', 'rlut', 'rootd', 'rsds', 'rsdt', 'rsus', 'rsut', 'sfcWind',
#'sfcWindmax', 'sfcWindmaxmax', 'sftgif', 'sftlf', 'sic', 'slev', 'slw', 'snc',
#'snd', 'snm', 'snownc', 'snw', 'sst', 'sund', 'ta1000', 'ta200', 'ta300',
#'ta400', 'ta500', 'ta600', 'ta700', 'ta850', 'ta875', 'ta900', 'ta925',
#'ta950', 'ta975', 'tas', 'tasmax', 'tasmaxts', 'tasmin', 'tasmints', 'tauu',
#'tauv', 'ts', 'tsmax', 'tsmin', 'tsos', 'u200', 'u500', 'u850', 'ua1000',
#'ua200', 'ua300', 'ua400', 'ua500', 'ua600', 'ua700', 'ua850', 'ua875',
#'ua900', 'ua925', 'ua950', 'ua975', 'uas', 'ustar', 'v200', 'v500', 'v850',
#'va1000', 'va200', 'va300', 'va400', 'va500', 'va600', 'va700', 'va850',
#'va875', 'va900', 'va925', 'va950', 'va975', 'vas', 'wsgsmax', 'zfull',
#'zg1000', 'zg200', 'zg300', 'zg350', 'zg400', 'zg450', 'zg500', 'zg550',
#'zg600', 'zg650', 'zg700', 'zg750', 'zg800', 'zg850', 'zg875', 'zg900',
#'zg925', 'zg950', 'zg975', 'zmla'])

group_time = (['_DRS_Dproject', '_DRS_Dproduct', '_DRS_domain',
'_DRS_Dinstitution', '_DRS_model', '_DRS_experiment', '_DRS_ensemble',
'_DRS_rcm', '_DRS_rcm_version', '_DRS_Dfrequency'])

group_fx = (['_DRS_Dproject', '_DRS_Dproduct', '_DRS_domain',
'_DRS_Dinstitution', '_DRS_model', '_DRS_experiment', '_DRS_rcm',
'_DRS_rcm_version'])

group_latest_versions = (['_DRS_Dproject', '_DRS_Dproduct', '_DRS_domain',
'_DRS_Dinstitution', '_DRS_model', '_DRS_experiment', '_DRS_ensemble',
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
