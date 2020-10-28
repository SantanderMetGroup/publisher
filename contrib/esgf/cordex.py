#!/usr/bin/env python

import os
import sys
import numpy as np
import pandas as pd

import esgf

_help = '''Usage:
    cordex.py [options] DATAFRAME

Options:
    -h, --help                      Show this message and exit.
    -d, --dest DESTINATION          Destination for HDF5 files (default is working directory).
    --variable-col VARIABLE         Column name where the variable is found (default is _DRS_variable).
    --latest VERSION_COLUMN         Column name to identify latest versions.

    --group-time FACETS             Comma separated facets that group time periods of variables.
    --group-fx FACETS               Comma separated facets that, given a group grouped by --group-time, return it's fxs.
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

#group_latest_versions = (['_DRS_Dproject', '_DRS_Dproduct', '_DRS_domain',
#'_DRS_Dinstitution', '_DRS_model', '_DRS_experiment', '_DRS_ensemble',
#'_DRS_rcm', '_DRS_rcm_version', '_DRS_Dfrequency', '_DRS_variable'])


def parse_args(argv):
    args = {
        'dest': os.path.join(os.getcwd(), 'unnamed.hdf'),
        'variable_col': '_DRS_variable',
        'latest': None,
        'group_time': 'project_id,product,CORDEX_domain,institute_id,driving_model_id,experiment_id,driving_model_ensemble_member,model_id,rcm_version_id,frequency',
        'group_fx': 'project_id,product,CORDEX_domain,institute_id,driving_model_id,experiment_id,model_id,rcm_version_id',
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
            args['dataframe'] = sys.argv[position]
            position+=1

    return args

if __name__ == '__main__':
    args = parse_args(sys.argv)

    if args['dataframe'] is None:
        print(_help)
        sys.exit(1)

    df = pd.read_hdf(args['dataframe'], 'df')

    if args['latest'] is not None:
        df = esgf.get_latest_versions(df, group_time, args['latest'])

    # netcdfs from CAS-22_NCC-NorESM1-M_rcp26_r0i0p0_GERICS-REMO2015 have ensemble before extension
    # set every _DRS_period column of fixed variables to None to prevent more failures
    #df.loc[df[('GLOBALS', '_DRS_Dfrequency')] == 'fx', ('GLOBALS', '_DRS_period')] = None

    df[('time', 'units')] = df[('time', 'units')].str.replace(' UTC', '')

    # Be sure that df has tracking_id column
    if not (('GLOBALS', 'tracking_id') in df.columns):
        df[('GLOBALS', 'tracking_id')] = None

    # Add institute to RCMModelName (institute-rcm)
    for r in df.index:
        if df.loc[r, ('GLOBALS', '_DRS_Dinstitute')] not in df.loc[r, ('GLOBALS', '_DRS_rcm')]:
            df.loc[r, ('GLOBALS', '_DRS_rcm')] = \
                '-'.join([df.loc[r, ('GLOBALS', '_DRS_Dinstitute')], df.loc[r, ('GLOBALS', '_DRS_rcm')]])

    # It appears that some files are duplicated under different DRS, remove all
    # ex: find /oceano/gmeteo/DATA/ESGF/REPLICA/DATA/cordex/output/SEA-22/RU-CORE/MPI-M-MPI-ESM-MR/rcp45/r1i1p1/ -type f -name pr_SEA-22_MPI-M-MPI-ESM-MR_rcp45_r1i1p1_ICTP-RegCM4-3_v4_day_2006010112-2006013112.nc
    df[('GLOBALS', 'filename')] = df[('GLOBALS', 'localpath')].apply(lambda x: os.path.basename(x))
    df = df.drop_duplicates(subset=[('GLOBALS', 'filename')])

    # for CORDEX_output_WAS-22_NCC-NorESM1-M_rcp26_r1i1p1_CLMcom-ETH-COSMO-crCLIM-v1-1_v1_day
    # precipitation time values were added decimals when converted to float so I limit number of decimals
    df[('time', '_values')] = df[('time', '_values')].apply(lambda x: np.round(x, decimals=6))

    # CORDEX_output_SAM-44_MPI-M-MPI-ESM-MR_rcp85_r1i1p1_ICTP-RegCM4-3_v4_day has
    # incorrect (not monotonically increasing) time coordinate, generate manually
    subset = ((df[('time', '_values')].apply(
                lambda a: not np.all(a[1:] >= a[:-1]) if not np.isnan(a).all() else False)) &
              (df[('GLOBALS', '_DRS_Dmodel')] == 'MPI-M-MPI-ESM-MR') &
              (df[('GLOBALS', '_DRS_Ddomain')] == 'SAM-44') &
              (df[('GLOBALS', '_DRS_Dfrequency')] == 'day'))
    df.loc[subset, ('time', '_values')] = (df.loc[subset, ('time', '_values')]
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

    # ncs from NAM-44_MOHC-HadGEM2-ES_rcp85_r1i1p1_NCAR-WRF_v3-5-1 end at 10:30 instead
    # of 12:00, fix manually last time step
    # not an error but climate4r uses time in seconds to detect daily data (I think)
    # also happens for CORDEX-NAM-44_MPI-M-MPI-ESM-LR_rcp85_r1i1p1_UA-WRF_v3-5-1
    time_diff = df[('time', '_values')].apply(
        lambda a: len(np.unique(np.diff(a))) != 1 if not np.isnan(a).all() else False)
    subset = ((df[('GLOBALS', '_DRS_Dfrequency')] == 'day') &
              (df[('GLOBALS', '_DRS_Ddomain')].str.match('NAM-44|NAM-22')) &
              (time_diff))
    df.loc[subset, ('time', '_values')] = (
        df.loc[subset, ('time', '_values')].apply(lambda a: np.arange(a[0], a[0]+len(a))))

    # AFR data with rotated_pole:grid_north_pole_longitude == -180. set to 0
    rotated_grids = ([
        ('rotated_pole', 'grid_north_pole_longitude'),
        ('rotated_latitude_longitude', 'grid_north_pole_longitude')])
    for grid_north_pole_longitude in rotated_grids:
        if grid_north_pole_longitude in df.columns:
            subset = (((df[grid_north_pole_longitude] == -180.0) | (df[grid_north_pole_longitude] == 180.0)) &
                       df[('GLOBALS', '_DRS_Ddomain')].str.match('AFR-'))
            df.loc[subset, grid_north_pole_longitude] = 0

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
