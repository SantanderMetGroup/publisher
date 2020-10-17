import os
import sys
import re
import pandas as pd
import numpy as np
import netCDF4
import cftime

# All fx variables in ESGF
vars_fx = (['areacella', 'areacello', 'areacellr', 'basin',
           'deptho', 'gridspec', 'hfgeou', 'lm', 'mrsofc',
           'orog', 'orograw', 'rootd', 'sftgif', 'sftlf',
           'sftof', 'thkcello', 'volcello', 'zfull'])

def render(df, dest):
    os.makedirs(os.path.dirname(dest), exist_ok=True)

    import warnings
    warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

    store = pd.HDFStore(dest)
    store['df'] = df
    store.close()

    return dest

# Make time values start from same units and calendar
def fix_time_values(df, facets, variable_col):
    how_to_group = [('GLOBALS', f) for f in facets.split(',')]

    for group_name, group in df[~df[('GLOBALS', variable_col)].isin(vars_fx)].groupby(how_to_group):
        i = ('GLOBALS', '_DRS_period1')
        reference = group.sort_values(i).iloc[0]
        reference_units = reference[('time', 'units')]
        reference_calendar = reference[('time', 'calendar')]

        subset = [('time', 'units'), ('time', 'calendar'), ('time', '_values')]
        cftimes = group[subset].apply(lambda row:
            cftime.num2date(row[('time', '_values')], row[('time', 'units')], row[('time', 'calendar')]), axis=1)
        df.loc[group.index, ('time', '_values')] = cftimes.apply(
            lambda dates: cftime.date2num(dates, reference_units, reference_calendar))

    return df

def get_latest_versions(df, facets, version_column):
    df[('GLOBALS', 'filename')] = df[('GLOBALS', 'localpath')].apply(lambda x: os.path.basename(x))
    how_to_group = [('GLOBALS', f) for f in facets.split(',')]

    latests = []
    for _, group in df.groupby(how_to_group):
        latests.append(group.nlargest(1, ('GLOBALS',version_column), keep='all'))

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
