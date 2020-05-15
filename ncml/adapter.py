import re
import sys, os
import pandas as pd
import netCDF4, cftime
from datetime import datetime

from ncml.reader import *

class Adapter():
	def __init__(self, name, template, groupby):
		self.name = name
		self.template = template
		self.groupby = groupby.split(',')
		self.reader = NetcdfMetadataReader()

	def filter_fx(self, df):
		raise NotImplementedError

	def get_fxs(self, df, facets, values):
		raise NotImplementedError

	def get_time_values(self, df):
		raise NotImplementedError

	def preprocess(self, df):
		raise NotImplementedError

	def test(self, df, ncml):
		raise NotImplementedError

class EsgfNcmlAdapter(Adapter):
	def get_latest_versions(self, df):
		raise NotImplementedError

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
		# Clean whitespaces from global attributes
		preprocessed['GLOBALS'] = preprocessed['GLOBALS'].applymap(lambda x: x.strip() if isinstance(x, str) else x)
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
			'mon': '%Y%m',
			'day': '%Y%m%d'
		}
	
		first = df[('GLOBALS', 'localpath')].iloc[0]
		last = df[('GLOBALS', 'localpath')].iloc[-1]
		p = '[0-9]+-[0-9]+'
		pfirst = re.findall(p, first)[-1]
		plast = re.findall(p, last)[-1]
		frequency_format = formats[df[('GLOBALS', 'frequency')].iloc[0]]
	
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
		if ('time', 'regular') in df.columns:
			return not df[('time', 'regular')].all()
		else:
			return False
	
	def test_different_time_units(self, df):
		return len(df[('time', 'units')].unique()) > 1
