#!/usr/bin/env python

import re
import os, sys
import argparse
import numpy as np
import pandas as pd

import netCDF4, cftime
from datetime import datetime
from jinja2 import Environment, FileSystemLoader, select_autoescape
import tqdm

import adapters

def to_ncml(name, template, **kwargs):
	templates = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'data')
	env = Environment(loader=FileSystemLoader(templates), autoescape=select_autoescape(['xml']))

	t = env.get_template(template)
	with open(name, 'w+') as fh:
		fh.write(t.render(**kwargs)) 

	print(name)

def get_data(files, root):
	print('Getting data from .nc files')
	if args.project.lower() == 'cordex_providers':
		adapter = adapters.get_adapter(args.project)
		facets_all = list(map(adapter.ncml_facets, files))

	for n, f in tqdm.tqdm(enumerate(files)):
		drs = os.path.relpath(f, root)

		if args.project.lower() == 'cordex_providers':
			facets = facets_all[n].split('/')
		else:
			facets = os.path.dirname(drs).split('/')

		# file data
		size = os.stat(f).st_size

		# nc data
		with netCDF4.Dataset(f) as ds:
			if ds.frequency == "fx":
				values = [np.nan] * 5
			else:
				time = ds.variables['time']
				ncoords = time.size
				units = time.units

				value0 = time[0].data.item()
				value1 = time[1].data.item()

				if ds.frequency == 'mon':
					# check if time is regular within the file
					# and if it is return it's value, else np.nan
					u = np.unique(np.diff(time))
					regular = u[0] if u.size == 1 else np.nan
				else:
					regular = 0

				values = [ncoords, units, value0, value1-value0, regular]
		yield [size] + facets + values

def test_missing_nc(df, ncml, variable):
	formats = {
		'Amon': '%Y%m',
		'day': '%Y%m%d',
		'mon': '%Y%m'
	}

	first = df.index[0]
	last = df.index[-1]
	p = '[0-9]+-[0-9]+'
	pfirst = re.findall(p, first)[-1]
	plast = re.findall(p, last)[-1]
	frequency_format = formats[df.loc[first].frequency]

	ideal_start = datetime.strptime(pfirst.split('-')[0], frequency_format)
	ideal_end = datetime.strptime(plast.split('-')[1], frequency_format)
	ideal = pd.date_range(ideal_start, ideal_end, freq='MS').to_list()

	real = []
	for f in df.index:
		fdates = re.findall(p, f)[-1].split('-')
		fstart_date = datetime.strptime(fdates[0], frequency_format)
		fend_date = datetime.strptime(fdates[1], frequency_format)

		real.extend(pd.date_range(fstart_date, fend_date, freq='MS'))

	return ideal != real

def test_regular_time(df, ncml, variable):
	return np.isnan(df.time_regular).any()

def test_different_time_units(df, ncml, variable):
	return len(df.time_units.unique()) > 1

def get_time_values(df):
	time_values = []

	for file in df.index:
		with netCDF4.Dataset(file) as ds:
			time_values.extend(ds.variables['time'][:])

	return time_values

def select_variables(df, variables_ncmls):
	posi_index = []

	for var_ncml in variables_ncmls:
		if var_ncml in df.variable.values:
			posi_index.append(np.where(np.in1d(df.variable.values, var_ncml))[0])

	posi_index_flat = [item for sublist in posi_index for item in sublist]
	posi_index_flat = np.sort(posi_index_flat)
	df_var_ncml = df.iloc[posi_index_flat]
	return df_var_ncml

def main(args):
	adapter = adapters.get_adapter(args.project)

	if adapter is None:
		sys.exit(1)

	# dataframe columns
	facets = adapter.facets
	file_info = ['size']
	time = ['time_ncoords', 'time_units', 'time_start', 'time_increment', 'time_regular']

	# index
	files = sys.stdin.read().splitlines()
	rels = [os.path.relpath(f, args.root) for f in files]
	if args.project.lower()=='cordex_providers': rels=files
	versions = map(adapter.drs_to_version, rels)
	variables = map(adapter.drs_to_var, rels)
	ncmls = map(adapter.drs_to_ncml, rels)

	i = pd.MultiIndex.from_tuples(list(zip(ncmls, variables, versions, files)), names=['ncmls','variables', 'versions', 'files'])
	cs = file_info + facets + time
	df = pd.DataFrame(get_data(files, args.root), index=i, columns=cs).sort_index(level=['files', 'versions'])

	if args.variables:#select vaiables in the dataframe
		variables_ncmls = args.variables.split(',')
		df = select_variables(df, variables_ncmls)

	# free memory
	del files
	del variables
	del versions
	del ncmls
	del rels
	
	print('Testing data and generating ncmls')
	for ncml in tqdm.tqdm(df[~df.variable.isin(adapter.fx)].index.unique(level='ncmls')):
		template = args.template
		for variable in df.loc[ncml].index.unique(level='variables'):
			latest = df.loc[(ncml, variable)].index.get_level_values(level='versions')[-1]
			if variable not in adapter.fx:
				joinExisting = df.loc[ncml, variable, latest]

				try:
					is_missing = test_missing_nc(joinExisting, ncml, variable)
					is_irregular = test_regular_time(joinExisting, ncml, variable)
					is_time_relative_to_file = test_different_time_units(joinExisting, ncml, variable)

					if is_missing:
						print('{},{},Missing'.format(ncml, variable), file=sys.stderr)

					if is_time_relative_to_file:
						print('{},{},DifferentTimeUnits'.format(ncml, variable), file=sys.stderr)

					if is_irregular:
						template = args.project.lower() + '.notime.ncml.j2'
						if args.project.lower() == 'cordex_providers': template = 'cordex.notime.ncml.j2'
						print('{},{},NoEquallySpacedTime'.format(ncml, variable), file=sys.stderr)

				except Exception as e:
					print('{},{},Exception'.format(ncml, variable), file=sys.stderr)
					print(e, file=sys.stderr)

		if args.project.lower() == 'cordex_providers':
			d = dict(zip(adapter.ncml_facets2(), ncml.split('_')))
		else:
			d = dict(zip(adapter.ncml_facets(), ncml.split('_')))


		dest = args.dest.format(**d)
		os.makedirs(os.path.dirname(dest), exist_ok=True)

		d = adapter.get_fx_dict(d)
		fxs = df[df.variable.isin(adapter.fx)].loc[(df[d.keys()] == pd.Series(d)).all(axis=1)]
		fxs.index = fxs.index.droplevel(0)

		# hardcoded time values, only for monthly irregular datasets
		if (df.loc[ncml].frequency.isin(['Amon', 'mon'])).all():
			a_var = df.loc[ncml].index.get_level_values(level='variables')[0]
			a_version = df.loc[ncml,a_var].index.get_level_values(level='versions')[-1]
			time_values = get_time_values(df.loc[ncml, a_var, a_version])
		else:
			time_values = []

		params = {
			'df': df.loc[ncml],
			'fxs': fxs,
			'time_values': time_values
		}
	
		to_ncml(dest, template, **params)

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Read a csv formatted table of facets and files and generate NcMLs')
	parser.add_argument('--project', dest='project', type=str, help='CMIP5, CMIP6, CORDEX, CORDEX_PROVIDERS (without DRS path structure)...')
	parser.add_argument('--dest', dest='dest', type=str, help='Full path to destination file using formatted strings.')
	parser.add_argument('--template', dest='template', type=str, default='cmip.ncml.j2', help='Template file')
	parser.add_argument('--group-spec', dest='group_spec', type=str, help='Comma separated facet names, e.g "project,product,model"')
	parser.add_argument('--aggregation-spec', dest='aggregation_spec', type=str, default='variable', help='Comma separated facet names.')
	parser.add_argument('--drs', dest='drs', type=str, help='Directory Reference Syntax: e.g. project/product/model/...')
	parser.add_argument('--root', dest='root', type=str, help='Directory substring before DRS')
	parser.add_argument('--variables', dest='variables', type=str, help='Variables selected to include in the ncmls')
	args = parser.parse_args()

	main(args)
