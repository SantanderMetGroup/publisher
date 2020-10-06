import os
import sys
import re
import pandas as pd
import numpy as np
import netCDF4
import cftime

def get_drs_df(localpaths, drs):
    ndrs = len(drs.split(','))
    drs_df = localpaths.apply(lambda x: os.path.splitext(x)[0]).str.split('[/_]', expand=True).iloc[:, -ndrs:]
    return drs_df

def include_drs(df, drs, prefix=None):
    if prefix is None:
        prefix = '_DRS_'

    drs_list = drs.split(',')
    ndrs_list = len(drs_list)
    localpaths = df[('GLOBALS', 'localpath')].apply(lambda x: os.path.splitext(x)[0])
    drs_df = localpaths.str.split('[/_]', expand=True).iloc[:, -ndrs_list:]
    drs_df.columns = [('GLOBALS', ''.join([prefix, f])) for f in drs_list]
    return pd.concat([df, drs_df], axis=1)

def group(df, group_time, group_fx):
    vars_fx = (['areacella', 'areacello', 'areacellr', 'basin',
               'deptho', 'gridspec', 'hfgeou', 'lm', 'mrsofc',
               'orog', 'orograw', 'rootd', 'sftgif', 'sftlf',
               'sftof', 'thkcello', 'volcello', 'zfull'])

    how_to_group = [('GLOBALS', facet) for facet in group_time]
    time_groups = df[~df[('GLOBALS', '_DRS_variable')].isin(vars_fx)].groupby(how_to_group)

    for name, group in time_groups:
        # for each group of time variables we have
        # to include corresponding fx variables
        d = dict(zip(group_time, name))
        filter_dict = {k: d[k] for k in group_fx} 
        all_fxs = df[df[('GLOBALS', '_DRS_variable')].isin(vars_fx)]
        group_fxs = all_fxs.loc[(df['GLOBALS'][filter_dict.keys()] == pd.Series(filter_dict)).all(axis=1)]

        # this would be the full dataset
        dataset = (pd.concat([group, group_fxs])
                     .sort_values(by=[('GLOBALS', '_DRS_variable'), ('GLOBALS', 'localpath')])
                     .reset_index(drop=True))

        yield dataset

def render(df, dest):
    os.makedirs(os.path.dirname(dest), exist_ok=True)

    import warnings
    warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

    store = pd.HDFStore(dest)
    store['df'] = df
    store.close()

    # I should definetly return the path, not print it here...
    print(dest)

def fix_time_values(df, facets):
    # So, I have collected all time values but I have to make
    # them start from the same reference
    how_to_group = [('GLOBALS', f) for f in facets]
    no_fx = df[('GLOBALS', '_DRS_Dfrequency')] != 'fx'

    # Convert empty arrays to string
    fx = df[('GLOBALS', '_DRS_Dfrequency')] == 'fx'
    df.loc[fx, ('time', 'values')] = ''

    # Convert non empty arrays to string
    for group_name, group in df[no_fx].groupby(how_to_group):
        i = ('GLOBALS', 'period1')
        reference = group.sort_values(i).iloc[0]
        reference_units = reference[('time', 'units')]
        reference_calendar = reference[('time', 'calendar')]

        subset = [('time', 'units'), ('time', 'calendar'), ('time', 'values')]
        cftimes = group[subset].apply(lambda x: list(map(
            lambda y: cftime.num2date(y, x[('time', 'units')], x[('time', 'calendar')]), x[('time', 'values')])), axis=1)
        times = (cftimes.apply(lambda x: cftime.date2num(x, reference_units, reference_calendar))
                        .apply(lambda x: list(map(str, list(x))))
                        .apply(lambda x: ' '.join(x)))

        df.loc[group.index, ('time', 'values')] = times

    return df

def get_time_values(df, facets):
    df[('time', 'values')] = df.apply(time_values, axis=1)
    return df

def time_values(series):
    f = series[('GLOBALS', 'localpath')]
    try:
        with netCDF4.Dataset(f) as ds:
            if 'time' in ds.variables:
                return ds.variables['time'][:]
            else:
                return []
    except Exception as err:
        print("Error while reading time values of {0}".format(f), file=sys.stderr)
        print("Error: {0}".format(err), file=sys.stderr)
        sys.exit(1)

def get_variable(variable, path):
    try:
        with netCDF4.Dataset(path) as ds:
            if variable in ds.variables:
                return ds.variables[variable][:]
            else:
                return []
    except Exception as err:
        print("Error while reading variable {0} values of {1}".format(variable, path), file=sys.stderr)
        print("Error: {0}".format(err), file=sys.stderr)
        sys.exit(1)

def get_latest_versions(df, facets):
    how_to_group = [('GLOBALS', f) for f in facets]

    latests = []
    for _, group in df.groupby(how_to_group):
        latests.append(group.nlargest(1, ('GLOBALS', 'nversion'), keep='all'))

    latest_versions = pd.concat(latests)

    # So, now we have latest versions but it's possible that some files from
    # different versions need to be included, since with some models, the time
    # series for a variable is split accross different versions
    no_latest_versions = df.loc[df.index.difference(latest_versions.index), :]

    if len(no_latest_versions) > 0:
        # Need to do this in case more than two versions exist
        latest_no_latest_versions = get_latest_versions(no_latest_versions, facets)

        # Drop filename that already exist in newer versions
        to_be_added = latest_no_latest_versions[~latest_no_latest_versions[('GLOBALS', 'filename')].isin(latest_versions[('GLOBALS', 'filename')])]

        return pd.concat([latest_versions, to_be_added])

    return latest_versions

def test_missing_nc(df, facets):
    # https://pandas.pydata.org/docs/user_guide/timeseries.html#representing-out-of-bounds-spans
    def conv(x): 
        if len(str(x)) == 12: 
            return pd.Period(year=x // 1e8, month=x // 1e6 % 100, day=x // 1e4 % 100,
                             hour= x // 100 % 100, minute=x % 100, freq='T') 
        elif len(str(x)) == 8: 
            return pd.Period(year=x // 1e4, month=x // 100 % 100, day=x % 100, freq='D') 
        elif len(str(x)) == 6: 
            return pd.Period(year=x // 100, month=x % 100, freq='M') 
        else: 
            raise Exception("Unknown date format during test_missing_nc")

    how_to_group = [('GLOBALS', f) for f in facets]
    for group_name, group in df.groupby(how_to_group):
        dates = group[[('GLOBALS', 'period1'), ('GLOBALS', 'period2')]].applymap(conv)
        initial_dates = dates[('GLOBALS', 'period1')].iloc[1:].reset_index(drop=True)
        end_dates = (dates[('GLOBALS', 'period2')] + 1).iloc[:-1].reset_index(drop=True)
        if not (initial_dates == end_dates).all():
            print('{},Missing'.format('_'.join(group_name)), file=sys.stderr)
