import re
import sys, os
import numpy as np
import pandas as pd
import netCDF4, cftime
from datetime import datetime
from lxml import etree
from jinja2 import Environment, FileSystemLoader, select_autoescape

from ncml.adapter import NetcdfAdapter as NcmlAdapter
from catalog.adapter import Adapter as CatalogAdapter

class EsgfCatalogAdapter(CatalogAdapter):
    def __init__(self, dest):
        if dest is None:
            self.dest = os.getcwd()
        else:
            self.dest = dest

        # Override in child classes
        self.namespace = ''
        self.template = ''
        self.root_name = ''
        self.root_template = ''
        self.location = ''

        templates = os.path.join(os.path.dirname(__file__), 'templates')
        self.env = self.setup_jinja(templates)

    def process_dataset(self, dataset):
        ext = os.path.splitext(dataset)[1]
        if ext == ".ncml":
            d = self.process_ncml(dataset)
        else:
            d = self.process_nc(dataset)

        return d

    def process_catalog(self, catalog):
        tree = etree.parse(catalog)
        namespaces = {'unidata': 'http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0'}
        name = tree.xpath('/unidata:catalog/@name', namespaces=namespaces)[0]

        return {
            'file': catalog,
            'title': name,
            'size': self.catalog_size(catalog),
            'last_modified': datetime.fromtimestamp(os.stat(catalog).st_mtime)
        }

    def catalog(self, catalog, datasets):
        path = os.path.join(self.dest, catalog)
        template = self.env.get_template(self.template)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w+') as fh:
            fh.write(template.render(
                name=catalog,
                datasets=datasets,
                namespace=self.namespace,
                location=self.location))

        return path

    def root_catalog(self, refs):
        path = self.dest
        template = self.env.get_template(self.root_template)

        with open(path, 'w+') as fh:
            fh.write(template.render(catalogs=refs, namespace=self.namespace, name=self.root_name))

        return path

    def process_ncml(self, ncml):
        namespaces = {'unidata': 'http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2'}
        tree = etree.parse(ncml)

        basename = os.path.basename(ncml)
        name = os.path.splitext(basename)[0]
        ext = os.path.splitext(basename)[1]
        last_modified = datetime.fromtimestamp(os.stat(ncml).st_mtime)
        size = tree.xpath(
            '/unidata:netcdf/unidata:attribute[@name="size"]',
            namespaces=namespaces)[0]
        primary_variables = tree.xpath(
            '/unidata:netcdf/unidata:attribute[@name="primary_variables"]',
            namespaces=namespaces)[0]

        return {
            'file': ncml,
            'name': name,
            'last_modified': last_modified,
            'service': 'virtual',
            'ext': ext,
            'size': int(size.attrib['value']),
            'primary_variables': primary_variables.attrib['value']
        }

    def process_nc(self, nc):
        basename = os.path.basename(nc)
        name = os.path.splitext(basename)[0]
        ext = os.path.splitext(basename)[1]
        last_modified = datetime.fromtimestamp(os.stat(nc).st_mtime)
        size = os.stat(nc).st_size

        return {
            'file': nc,
            'name': name,
            'last_modified': last_modified,
            'size': size,
            'service': 'all',
            'ext': ext
        }

    def catalog_size(self, catalog):
        namespaces = {'unidata': 'http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0'}
        tree = etree.parse(catalog)
        sizes = tree.xpath('//unidata:dataSize', namespaces=namespaces)

        total_size = 0
        for s in sizes:
            total_size += int(s.text)

        return total_size

class EsgfNcmlAdapter(NcmlAdapter):
    def __init__(self):
        # I use the same set of fx variables for cordex, cmip5 and cmip6
        self.vars_fx = (['areacella', 'areacello', 'areacellr', 'basin',
                        'deptho', 'gridspec', 'hfgeou', 'lm', 'mrsofc',
                        'orog', 'orograw', 'rootd', 'sftgif', 'sftlf',
                        'sftof', 'thkcello', 'volcello', 'zfull'])

        templates = os.path.join(os.path.dirname(__file__), 'templates')
        self.env = self.setup_jinja(templates)

    def read(self, file):
        attrs = super().read(file)
        dirname = os.path.dirname(file)
        attrs['GLOBALS']['version'] = dirname.split('/')[-1]

        with netCDF4.Dataset(file) as ds:
            # Required metadata for the time variable
            if 'time' in ds.variables:
                time = ds.variables['time']
                ncoords = time.size
                value0 = time[0].data.item()
                value1 = time[1].data.item()

                attrs['time']['ncoords'] = time.size
                attrs['time']['value0'] = value0
                attrs['time']['increment'] = value1-value0

                # check if time is regular within the file
                u = np.unique(np.diff(ds.variables['time']))
                attrs['time']['regular'] = u.size==1

        return attrs

    def preprocess(self, df):
        # Add nversion column
        df.loc[:, ('GLOBALS', 'nversion')] = df.loc[:, ('GLOBALS', 'version')].str.replace('[a-zA-Z]', '').astype(int)
        # Latest versions
        preprocessed = self.get_latest_versions(df)
        # Clean whitespaces from global attributes
        preprocessed['GLOBALS'] = preprocessed['GLOBALS'].applymap(lambda x: x.strip() if isinstance(x, str) else x)

        return preprocessed

    def group(self, df):
        how_to_group = [('GLOBALS', facet) for facet in self.group_time]
        time_groups = df[~df[('GLOBALS', 'variable_id')].isin(self.vars_fx)].groupby(how_to_group)

        # for each group of time variables we have
        # to include corresponding fx variables
        for name, group in time_groups:
            d = dict(zip(self.group_time, name))
            filter_dict = {k: d[k] for k in self.group_fx} 
            all_fxs = df[df[('GLOBALS', 'variable_id')].isin(self.vars_fx)]
            group_fxs = all_fxs.loc[(df['GLOBALS'][filter_dict.keys()] == pd.Series(filter_dict)).all(axis=1)]

            yield pd.concat([group, group_fxs]).sort_values(by=[('GLOBALS', 'variable_id'), ('GLOBALS', 'localpath')])

    def get_latest_versions(self, df):
        latests = []
        how_to_group = [('GLOBALS', f) for f in self.group_latest_versions]

        for _, group in df.groupby(how_to_group):
            latests.append(group.nlargest(1, ('GLOBALS', 'nversion'), keep='all'))

        return pd.concat(latests)

    def to_ncml(self, df):
        # Obtain facet values from first netCDF file
        d = dict(df[~df[('GLOBALS', 'variable_id')].isin(self.vars_fx)]['GLOBALS'].iloc[0])
        path = self.dest.format(**d)
        os.makedirs(os.path.dirname(path), exist_ok=True)

        # If monthly, obtain time dimension values from one of the variables
        time_values = []
        if (df[('GLOBALS', 'frequency')] == 'mon').any():
            a_variable = df[~df[('GLOBALS', 'variable_id')].isin(self.vars_fx)].iloc[0][('GLOBALS', 'variable_id')]
            files = df[df[('GLOBALS', 'variable_id')] == a_variable][('GLOBALS', 'localpath')]

            # I assume files is already sorted
            for f in files:
                with netCDF4.Dataset(f) as ds:
                    time_values.extend(ds.variables['time'][:])

            time_values = list(map(str, time_values))

        t = self.env.get_template(self.template)
        with open(path, 'w+') as fh:
            fh.write(t.render({'df': df, 'time_values': time_values}))

        return path

    def test(self, df, ncml):
        for variable in df[~df[('GLOBALS', 'variable_id')].isin(self.vars_fx)][('GLOBALS', 'variable_id')].unique():
            vdf = df[df[('GLOBALS', 'variable_id')] == variable]

            try:
                self.test_missing_nc(vdf, ncml, variable)
                #self.test_regular_time(vdf, ncml, variable)
                #self.test_different_time_units(vdf, ncml, variable)
            except Exception as e:
                print('{},{},TestException,{}'.format(ncml, variable, e), file=sys.stderr)

    def test_missing_nc(self, df, ncml, variable):
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

        localpaths = df[('GLOBALS', 'localpath')].sort_values()
        dates = (localpaths
                 .apply(lambda x: re.findall('[0-9]+-[0-9]+', x)[-1])
                 .str.split('-', expand=True)
                 .applymap(lambda x: conv(int(x))))
        initial_dates = dates[0].iloc[1:].reset_index(drop=True)
        end_dates = dates[1].iloc[:-1].reset_index(drop=True)
        deltas = end_dates - initial_dates

        if len(deltas.value_counts()) > 1:
            print('{},{},Missing'.format(ncml, variable), file=sys.stderr)

    def test_regular_time(self, df, ncml, variable):
        if ('time', 'regular') in df.columns:
            if not df[('time', 'regular')].all() and (df[('GLOBALS', 'frequency')] == 'mon').any():
                print('{},{},NoEquallySpacedTime'.format(ncml, variable), file=sys.stderr)

    def test_different_time_units(self, df, ncml, variable):
        if len(df[('time', 'units')].unique()) > 1:
            print('{},{},DifferentTimeUnits'.format(ncml, variable), file=sys.stderr)
