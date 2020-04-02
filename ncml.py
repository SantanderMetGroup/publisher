#!/usr/bin/env python

import os, sys
import argparse
import pandas as pd

from jinja2 import Environment, FileSystemLoader, select_autoescape

from ncml.adapter import *
from ncml.reader import *

from projects.cmip6 import Cmip6NcmlAdapter
from projects.cordexEsdm import EcearthCordexEsdmNcmlAdapter, InterimCordexEsdmNcmlAdapter

def to_ncml(name, template, **kwargs):
	templates = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'templates/ncmls')
	env = Environment(loader=FileSystemLoader(templates), autoescape=select_autoescape(['xml']))

	t = env.get_template(template)
	with open(name, 'w+') as fh:
		fh.write(t.render(**kwargs)) 

	print(name)

def get_data(reader, files):
	for f in files:
		metadata = reader.read(f)
		# https://stackoverflow.com/questions/24988131/nested-dictionary-to-multiindex-dataframe-where-dictionary-keys-are-column-label
		yield {(outerKey, innerKey): value for outerKey, innerDict in metadata.items() for innerKey, value in innerDict.items()}

def main(args):
	if args.adapter is None:
		adapter = Adapter(args.name, args.template, args.groupby)
	else:
		adapter = globals()[args.adapter]()

	files = sys.stdin.read().splitlines()
	df = pd.DataFrame(get_data(adapter.reader, files))
	df.columns = pd.MultiIndex.from_tuples(df.columns)

	time_variables = adapter.filter_fx(df)
	groupby_spec = adapter.groupby
	grouper = list(pd.MultiIndex.from_product([['GLOBALS'], groupby_spec]))

	# Each loop iteration generates a NcML
	for n,g in time_variables.groupby(grouper):
		# Generate NcML template variables
		preprocessed = adapter.preprocess(g)
		fxs = adapter.get_fxs(df, groupby_spec, n)
		time_values = adapter.get_time_values(preprocessed)

		# Interpolate NcML destination
		d = dict(zip(groupby_spec, n))
		name = adapter.name.format(**d)
		dest = os.path.join(args.dest, name)

		# Quality tests
		adapter.test(preprocessed, dest)

		# Generate NcML
		os.makedirs(os.path.dirname(dest), exist_ok=True)
		params = {'df': preprocessed, 'fxs': fxs, 'time_values': time_values}
		to_ncml(dest, adapter.template, **params)

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Read a list of files from stdin and generate NcMLs')
	parser.add_argument('--dest', dest='dest', required=True, type=str, help='Destination directory')
	parser.add_argument('--name', dest='name', type=str, help='f-string file name template')
	parser.add_argument('--template', dest='template', type=str, help='NcML template file')
	parser.add_argument('--groupby', dest='groupby', type=str, help='Comma separated facet names, e.g "project,product,model"')
	parser.add_argument('--adapter', dest='adapter', type=str, help='Adapter class')
	#parser.add_argument('--reader', dest='reader', type=str, help='Reader class')
	args = parser.parse_args()

	main(args)
