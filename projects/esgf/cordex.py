import os, sys
import re
import pandas as pd
import netCDF4
from jinja2 import Environment, FileSystemLoader, select_autoescape
from lxml import etree
from datetime import datetime

from projects.esgf.esgf import EsgfNcmlAdapter
from projects.esgf.esgf import EsgfCatalogAdapter

class CordexCatalogAdapter(EsgfCatalogAdapter):
	def __init__(self, dest):
		super().__init__()
		self.dest = dest
		self.namespace = 'devel/c3s34d/CORDEX'
		self.template = 'cordex.xml.j2'
		self.root_template = 'cordexRoot.xml.j2'

	def group(self, file):
		basename = os.path.basename(file)
		name = os.path.splitext(basename)[0]
		facets = name.split('_')
		grouper = [facets[i] for i in [0,1,2,3,4,6,7,8]]

		return '/'.join(grouper)

	# to be removed, it's here because right now cordex ncml contains no primary_variables attribute
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

		return {
			'file': ncml,
			'name': name,
			'last_modified': last_modified,
			'service': 'virtual',
			'ext': ext,
			'size': int(size.attrib['value'])
		}

class CordexNcmlAdapter(EsgfNcmlAdapter):
	def __init__(self, dest):
		self.dest = dest
		self.template = 'cordex.ncml.j2'

	def read(self, file):
		attrs = super().read(file)
		dirname = os.path.dirname(file)
		facets = dirname.split('/')
		attrs['GLOBALS']['variable_id'] = facets[-2]
		attrs['GLOBALS']['institution_id'] = facets[-9]
		return attrs

	def preprocess(self, df):
		df.loc[:, ('GLOBALS', 'nversion')] = df.loc[:, ('GLOBALS', 'version')].str.replace('[a-zA-Z]', '').astype(int)
		preprocessed = self.get_latest_versions(df)

		# Clean whitespaces from global attributes
		preprocessed['GLOBALS'] = preprocessed['GLOBALS'].applymap(lambda x: x.strip() if isinstance(x, str) else x)

		# Add institute_id to RCMModelName (institute_id-model_id)
		for r in preprocessed.index:
			if preprocessed.loc[r, ('GLOBALS', 'institute_id')] not in preprocessed.loc[r, ('GLOBALS', 'model_id')]:
				preprocessed.loc[r, ('GLOBALS', 'model_id')] = \
				'-'.join([preprocessed.loc[r, ('GLOBALS', 'institute_id')], preprocessed.loc[r, ('GLOBALS', 'model_id')]])

		return preprocessed

	def group(self, df):
		time_group = ['project_id', 'product', 'CORDEX_domain', 'institution_id', 'driving_model_id', 'experiment_id', 'driving_model_ensemble_member', 'model_id', 'rcm_version_id', 'frequency']
		fx_group = ['project_id', 'product', 'CORDEX_domain', 'institution_id', 'driving_model_id', 'experiment_id', 'model_id', 'rcm_version_id']
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
		facets = ['project_id', 'product', 'CORDEX_domain', 'institution_id', 'driving_model_id', 'experiment_id', 'driving_model_ensemble_member', 'model_id', 'rcm_version_id', 'frequency']
		directory = '{project_id}/{product}/{CORDEX_domain}/{institution_id}/{driving_model_id}/{experiment_id}/{model_id}/{rcm_version_id}/{frequency}'
		filename = '{project_id}_{product}_{CORDEX_domain}_{driving_model_id}_{experiment_id}_{driving_model_ensemble_member}_{model_id}_{rcm_version_id}_{frequency}.ncml'

		path = os.path.join(directory, filename)

		# Obtain facet values from first netCDF file
		d = dict(df[df[('GLOBALS', 'frequency')] != 'fx']['GLOBALS'][facets].iloc[0])
		return path.format(**d)

	def get_latest_versions(self, df):
		latests = []
		facets = ['project_id', 'product', 'CORDEX_domain', 'institution_id', 'driving_model_id', 'experiment_id', 'driving_model_ensemble_member', 'model_id', 'rcm_version_id', 'frequency', 'variable_id']
		how_to_group = [('GLOBALS', f) for f in facets]

		for _, group in df.groupby(how_to_group):
			latests.append(group.nlargest(1, ('GLOBALS', 'nversion'), keep='all'))

		return pd.concat(latests)
