#!/usr/bin/env python

import os
import sys
import numpy as np
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
    df.loc[df[('GLOBALS', '_DRS_Dfrequency')] == 'fx', ('GLOBALS', '_DRS_period')] = None
    df[('GLOBALS', 'period1')] = df[('GLOBALS', '_DRS_period')].str.split('-', expand=True).iloc[:,0].fillna(0).astype(int)
    df[('GLOBALS', 'period2')] = df[('GLOBALS', '_DRS_period')].str.split('-', expand=True).iloc[:,1].fillna(0).astype(int)

    df[('time', 'units')] = df[('time', 'units')].str.replace(' UTC', '')

    # Be sure that df has tracking_id column
    if not (('GLOBALS', 'tracking_id') in df.columns):
        df[('GLOBALS', 'tracking_id')] = None

    # Add institute to RCMModelName (institute-rcm)
    for r in df.index:
        if df.loc[r, ('GLOBALS', '_DRS_Dinstitution')] not in df.loc[r, ('GLOBALS', '_DRS_rcm')]:
            df.loc[r, ('GLOBALS', '_DRS_rcm')] = \
                '-'.join([df.loc[r, ('GLOBALS', '_DRS_Dinstitution')], df.loc[r, ('GLOBALS', '_DRS_rcm')]])

    # It appears that some files are duplicated under different DRS, remove all
    # ex: find /oceano/gmeteo/DATA/ESGF/REPLICA/DATA/cordex/output/SEA-22/RU-CORE/MPI-M-MPI-ESM-MR/rcp45/r1i1p1/ -type f -name pr_SEA-22_MPI-M-MPI-ESM-MR_rcp45_r1i1p1_ICTP-RegCM4-3_v4_day_2006010112-2006013112.nc
    df = df.drop_duplicates(subset=[('GLOBALS', 'filename')])

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
    print('* contrib/esgf/cordex.py on {0}'.format(args['dataframe']), file=sys.stderr)

    # If DRS is esgprep like (files/dVERSION) instead of synda like (vVERSION), convert to synda like
    df[('GLOBALS', 'synda_localpath')] = df[('GLOBALS', 'localpath')].str.replace('files/d([0-9]{6})', 'v\\1')

    # Parse DRS, if all ncs in df are fx frequency, we need to fix drs_df
    drs_df = esgf.get_drs_df(df[('GLOBALS', 'synda_localpath')], drs)
    if (drs_df.iloc[:,0].str.lower() != 'cordex').any():
        drs_df = drs_df.drop(axis=1, columns=[6])
        drs_df[27] = None # This is the 'period' facet, None for fx frequency
        print('Error: Dataframe {0} only contains fx datasets'.format(args['dataframe']), file=sys.stderr)
        sys.exit(1)

    drs_df.columns = [('GLOBALS', ''.join(['_DRS_', f])) for f in drs.split(',')]
    df = pd.concat([df, drs_df], axis=1)

    # Start cleaning stuff
    df = clean(df)
    df = esgf.get_latest_versions(df, group_latest_versions)

    # read coordinate variables
    df[('time', 'values')] = df.apply(lambda series: esgf.get_variable('time', series[('GLOBALS', 'localpath')]), axis=1)
    # Instead of rlon and rlat, coordinate variables are named x and y, so
    # I need the values to create rlon and rlat in the NcML (do I?)
    if '_d_x' in df.columns.get_level_values(0):
        df[('x', 'values')] = df.apply(lambda series: list(esgf.get_variable('x', series[('GLOBALS', 'localpath')])), axis=1)
    if '_d_y' in df.columns.get_level_values(0):
        df[('y', 'values')] = df.apply(lambda series: list(esgf.get_variable('y', series[('GLOBALS', 'localpath')])), axis=1)

    # for CORDEX_output_WAS-22_NCC-NorESM1-M_rcp26_r1i1p1_CLMcom-ETH-COSMO-crCLIM-v1-1_v1_day
    # precipitation time values were added decimals when converted to float so I limit number of decimals
    df[('time', 'values')] = df[('time', 'values')].apply(lambda x: np.round(x, decimals=6))

    # CORDEX_output_SAM-44_MPI-M-MPI-ESM-MR_rcp85_r1i1p1_ICTP-RegCM4-3_v4_day has
    # incorrect (not monotonically increasing) time coordinate, generate manually
    subset = ((df[('time', 'values')].apply(lambda a: not np.all(a[1:] >= a[:-1]))) &
              (df[('GLOBALS', '_DRS_Dmodel')] == 'MPI-M-MPI-ESM-MR') &
              (df[('GLOBALS', '_DRS_Ddomain')] == 'SAM-44') &
              (df[('GLOBALS', '_DRS_Dfrequency')] == 'day'))
    df.loc[subset, ('time', 'values')] = (df.loc[subset, ('time', 'values')]
                                            .apply(lambda a: np.arange(a[0], a[0]+len(a))))

    # ncs from cordex_output_NAM-44_UQAM_CCCma-CanESM2_historical begin with calendar gregorian_proleptic
    # but then use 365_day, so we set manually the values of gregorian_proleptic to 365_day
    subset = ((df[('GLOBALS', '_DRS_Dmodel')] == 'CCCma-CanESM2') &
              (df[('GLOBALS', '_DRS_Dexperiment')] == 'historical') &
              (df[('GLOBALS', '_DRS_Ddomain')] == 'NAM-44') &
              (df[('GLOBALS', '_DRS_Dfrequency')] == 'day') &
              (df[('GLOBALS', '_DRS_period')].str.split('-').str.get(0) == "19500101") &
              (df[('time', 'calendar')] == 'proleptic_gregorian'))
    df.loc[subset, ('time', 'calendar')] = '365_day'

    df = esgf.fix_time_values(df, group_latest_versions)

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
