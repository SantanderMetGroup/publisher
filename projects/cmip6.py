import os, sys
import re
import pandas as pd
import netCDF4
from datetime import datetime

from ncml.adapter import Adapter as NcmlAdapter
from ncml.reader import NetcdfMetadataReader
from catalog.adapter import Adapter as CatalogAdapter

class Cmip6CatalogAdapter(CatalogAdapter):
	def __init__(self):
		self.template = 'cmip6/cmip6.xml.j2'
		self.rtemplate = 'cmip6/root.xml.j2'

	def group(self, file):
		basename = os.path.basename(file)
		name = os.path.splitext(basename)[0]
		facets = name.split('_')
		grouper = [facets[i] for i in [0,1,2,3,4,6,7,8,9]]

		return '/'.join(grouper)

class Cmip6MetadataReader(NetcdfMetadataReader):
	#drs = ['project','product','institution','model','experiment','ensemble','frequency','variable','grid','version']

	def read(self, file):
		attrs = super().read(file)
		dirname = os.path.dirname(file)
		attrs['GLOBALS']['version'] = dirname.split('/')[-1]

		attrs['time']['regular'] = True
		with netCDF4.Dataset(file) as ds:
			if ('time' in ds.variables) & (ds.frequency == 'mon'):
				# check if time is regular within the file
				u = np.unique(np.diff(ds.variables['time']))
				regular = u.size == 1
				attrs['time']['regular'] = regular

		return attrs

class Cmip6NcmlAdapter(NcmlAdapter):
	def __init__(self):
		self.reader = Cmip6MetadataReader()

		self.directory = '{mip_era}/{activity_id}/{source_id}/{institution_id}/{experiment_id}/{grid_label}/{realm}/{table_id}/{frequency}'
		self.filename = '{mip_era}_{activity_id}_{source_id}_{institution_id}_{experiment_id}_{variant_label}_{grid_label}_{realm}_{table_id}_{frequency}.ncml'
		self.name = os.path.join(self.directory, self.filename)

		self.template = 'cmip6/cmip6.ncml.j2'
		self.groupby = ['mip_era', 'activity_id','source_id' ,'institution_id' ,'experiment_id' ,'variant_label' ,'grid_label', 'realm', 'table_id', 'frequency']

		self.fxs = ['areacella', 'areacellr', 'orog', 'sftlf', 'sftgif', 'mrsofc', 'rootd', 'zfull']
		self.fxs_facets = ['mip_era', 'activity_id','source_id' ,'institution_id' ,'experiment_id' ,'variant_label' ,'grid_label', 'realm']

	def filter_fx(self, df):
		return df[~df[('GLOBALS', 'variable_id')].isin(self.fxs)]

	def get_fxs(self, df, facets, values):
		d = dict(zip(facets, values))
		filter = {k: d[k] for k in self.fxs_facets}
		fxs = df[df[('GLOBALS', 'variable_id')].isin(self.fxs)].loc[(df['GLOBALS'][filter.keys()] == pd.Series(filter)).all(axis=1)]

		if len(fxs) == 0:
			return pd.DataFrame(columns=df.columns)
		else:
			return self.get_latest_versions(fxs)

	def get_latest_versions(self, df):
		latests = []

		for _, group in df.groupby(('GLOBALS', 'variable_id')):
			nversion = pd.Series(group[('GLOBALS', 'version')].str.replace('[a-zA-Z]', ''), dtype="int")
			group[('GLOBALS', 'version')] = nversion
			latests.append(group.nlargest(1, ('GLOBALS', 'version'), keep='all'))

		return pd.concat(latests)

	def get_time_values(self, df):
		if (df[('GLOBALS', 'frequency')] != 'mon').all():
			return []

		time_values = []
		variable = df[('GLOBALS', 'variable_id')].iloc[0]
	
		for file in df[df[('GLOBALS', 'variable_id')] == variable][('GLOBALS', 'localpath')]:
			with netCDF4.Dataset(file) as ds:
				time_values.extend(ds.variables['time'][:])
	
		return time_values

	def preprocess(self, df):
		preprocessed = self.get_latest_versions(df)
		preprocessed.sort_values(by=[('GLOBALS', 'variable_id'), ('GLOBALS', 'localpath')], inplace=True)
		preprocessed.reset_index(inplace=True)

		return preprocessed

	def test(self, df, ncml):
		for variable in df[('GLOBALS', 'variable_id')].unique():
			vdf = df[df[('GLOBALS', 'variable_id')] == variable]

			try:
				is_missing = self.test_missing_nc(vdf)
				is_irregular = self.test_regular_time(vdf)
				is_time_relative_to_file = self.test_different_time_units(vdf)

				if is_missing:
					print('{},{},Missing'.format(ncml, variable), file=sys.stderr)

				if is_time_relative_to_file:
					print('{},{},DifferentTimeUnits'.format(ncml, variable), file=sys.stderr)

				if is_irregular:
					print('{},{},NoEquallySpacedTime'.format(ncml, variable), file=sys.stderr)
			except Exception as e:
				print('{},{},Exception,{}'.format(ncml, variable, e), file=sys.stderr)

	def test_missing_nc(self, df):
		formats = {
			'Amon': '%Y%m',
			'day': '%Y%m%d'
		}
	
		first = df[('GLOBALS', 'localpath')].iloc[0]
		last = df[('GLOBALS', 'localpath')].iloc[-1]
		p = '[0-9]+-[0-9]+'
		pfirst = re.findall(p, first)[-1]
		plast = re.findall(p, last)[-1]
		frequency_format = formats[df[('GLOBALS', 'table_id')].iloc[0]]
	
		ideal_start = datetime.strptime(pfirst.split('-')[0], frequency_format)
		ideal_end = datetime.strptime(plast.split('-')[1], frequency_format)
		ideal = pd.date_range(ideal_start, ideal_end, freq='MS').to_list()
	
		real = []
		for f in df[('GLOBALS', 'localpath')]:
			fdates = re.findall(p, f)[-1].split('-')
			fstart_date = datetime.strptime(fdates[0], frequency_format)
			fend_date = datetime.strptime(fdates[1], frequency_format)
	
			real.extend(pd.date_range(fstart_date, fend_date, freq='MS'))
	
		return ideal != real
	
	def test_regular_time(self, df):
		return not df[('time', 'regular')].all()
	
	def test_different_time_units(self, df):
		return len(df[('time', 'units')].unique()) > 1
