import os, sys
import re
import numpy as np
import pandas as pd
import netCDF4
from jinja2 import Environment, FileSystemLoader, select_autoescape

from projects.esgf.esgf import EsgfNcmlAdapter
from projects.esgf.esgf import EsgfCatalogAdapter

class Cmip6CatalogAdapter(EsgfCatalogAdapter):
	def __init__(self, dest):
		super().__init__()
		self.namespace = 'devel/atlas/cmip6'
		self.template = 'cmip6.xml.j2'
		self.root_template = 'cmip6Root.xml.j2'
		self.dest = dest

	def group(self, file):
		basename = os.path.basename(file)
		name = os.path.splitext(basename)[0]
		facets = name.split('_')
		grouper = [facets[i] for i in [0,1,2,3,4,6]]

		return '/'.join(grouper)

class Cmip6NcmlAdapter(EsgfNcmlAdapter):
	def __init__(self, dest):
		super().__init__(dest)
		self.template = 'cmip6.ncml.j2'

	def preprocess(self, df):
		# Add nversion column
		df.loc[:, ('GLOBALS', 'nversion')] = df.loc[:, ('GLOBALS', 'version')].str.replace('[a-zA-Z]', '').astype(int)
		preprocessed = self.get_latest_versions(df)
		preprocessed = self.filter_grid_labels(df)

		# Clean whitespaces from global attributes
		preprocessed['GLOBALS'] = preprocessed['GLOBALS'].applymap(lambda x: x.strip() if isinstance(x, str) else x)

		# Fix AerChemMIP activity_id attribute
		i = ('GLOBALS', 'activity_id')
		preprocessed.loc[:, i] = preprocessed.loc[:, i].str.replace('ScenarioMIP AerChemMIP', 'ScenarioMIP')

		return preprocessed

	def group(self, df):
		time_group = ['mip_era', 'activity_id','source_id' ,'institution_id' ,'experiment_id' ,'variant_label' , 'realm', 'table_id', 'frequency', 'grid_label']
		fx_group = ['mip_era', 'activity_id','source_id' ,'institution_id' ,'experiment_id' ,'variant_label', 'realm']
		fx_vars = ['areacella', 'areacellr', 'orog', 'sftlf', 'sftgif', 'mrsofc', 'rootd', 'zfull']

		how_to_group = [('GLOBALS', facet) for facet in time_group]
		time_groups = df[df[('GLOBALS', 'frequency')] != 'fx'].groupby(how_to_group)

		# for each group of time variables we have
		# to include corresponding fx variables
		for name, group in time_groups:
			d = dict(zip(time_group, name))
			filter_dict = {k: d[k] for k in fx_group} 
			all_fxs = df[df[('GLOBALS', 'variable_id')].isin(fx_vars)]
			group_fxs = all_fxs.loc[(df['GLOBALS'][filter_dict.keys()] == pd.Series(filter_dict)).all(axis=1)]

			yield pd.concat([group, group_fxs]).sort_values(by=[('GLOBALS', 'variable_id'), ('GLOBALS', 'localpath')])

	def to_ncml(self, df):
		rel_path = self.get_ncml_path(df)
		path = os.path.join(self.dest, rel_path)
		os.makedirs(os.path.dirname(path), exist_ok=True)

		templates = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'templates')
		env = Environment(
			loader=FileSystemLoader(templates),
			autoescape=select_autoescape(['xml']),
			trim_blocks=True,
			lstrip_blocks=True)
		t = env.get_template(self.template)

		# If monthly, obtain time dimension values from one of the variables
		time_values = []
		if (df[('GLOBALS', 'frequency')] == 'mon').any():
			a_variable = df[df[('GLOBALS', 'frequency')] != 'fx'].iloc[0][('GLOBALS', 'variable_id')]
			files = df[df[('GLOBALS', 'variable_id')] == a_variable][('GLOBALS', 'localpath')]

			# I assume files is already sorted
			for f in files:
				with netCDF4.Dataset(f) as ds:
					time_values.extend(ds.variables['time'][:])

			time_values = list(map(str, time_values))

		with open(path, 'w+') as fh:
			fh.write(t.render({'df': df, 'time_values': time_values}))

		return path

	def get_ncml_path(self, df):
		facets = ['mip_era', 'activity_id','source_id' ,'institution_id' ,'experiment_id' ,'variant_label' , 'realm', 'table_id', 'frequency', 'grid_label']
		directory = 'cmip6/{activity_id}/{institution_id}/{source_id}/{experiment_id}/{table_id}'
		filename = 'cmip6_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{variant_label}_{table_id}.ncml'
		path = os.path.join(directory, filename)

		# Obtain facet values from first netCDF file
		d = dict(df[df[('GLOBALS', 'frequency')] != 'fx']['GLOBALS'][facets].iloc[0])
		return path.format(**d)

	def get_latest_versions(self, df):
		latests = []
		facets = ['mip_era', 'activity_id','source_id' ,'institution_id' ,'experiment_id' ,'variant_label' , 'realm', 'table_id', 'frequency', 'grid_label', 'variable_id']
		how_to_group = [('GLOBALS', f) for f in facets]

		for _, group in df.groupby(how_to_group):
			latests.append(group.nlargest(1, ('GLOBALS', 'nversion'), keep='all'))

		return pd.concat(latests)

	def filter_grid_labels(self, df):
		def gridlabel_to_int(grid_label):
			if grid_label == "gn":
				return 0
			elif grid_label == "gr":
				return 1
			else:
				# priority gn > gr > gr1 > gr2 > ...., 0 is greatest priority
				return int(re.sub("[^0-9]", "", grid_label)) + 1

		df[('GLOBALS', 'ngrid_label')] = df[('GLOBALS', 'grid_label')].apply(gridlabel_to_int)
		unique_grid_labels = []
		facets = ['mip_era', 'activity_id','source_id' ,'institution_id' ,'experiment_id' ,'variant_label' , 'realm', 'table_id', 'frequency']
		how_to_group = [('GLOBALS', f) for f in facets]

		for _,group in df.groupby(how_to_group):
			unique_grid_labels.append(group.nlargest(1, ('GLOBALS', 'ngrid_label'), keep='all'))

		return pd.concat(unique_grid_labels)
