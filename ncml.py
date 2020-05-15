#!/usr/bin/env python

import os, sys
import argparse
import pandas as pd

from jinja2 import Environment, FileSystemLoader, select_autoescape

from ncml.adapter import *
from ncml.reader import *

from projects.cmip6 import Cmip6NcmlAdapter
from projects.cordex import CordexNcmlAdapter
from projects.cordexEsdm import EcearthCordexEsdmNcmlAdapter, InterimCordexEsdmNcmlAdapter

# need to debug this
pd.options.mode.chained_assignment = None

def to_ncml(name, template, **kwargs):
	templates = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'templates/ncmls')
	env = Environment(loader=FileSystemLoader(templates), autoescape=select_autoescape(['xml']))

	t = env.get_template(template)
	with open(name, 'w+') as fh:
		fh.write(t.render(**kwargs)) 

	print(name)

def get_data(reader, files):
	for f in files:
		try:
			metadata = reader.read(f)
			# https://stackoverflow.com/questions/24988131/nested-dictionary-to-multiindex-dataframe-where-dictionary-keys-are-column-label
			yield {(outerKey, innerKey): value for outerKey, innerDict in metadata.items() for innerKey, value in innerDict.items()}
		except Exception as e:
			print('{},MetadataReadException,{}'.format(f, e), file=sys.stderr)

def main(args):
	if args.adapter is None:
		adapter = Adapter(args.name, args.template, args.groupby)
	else:
		adapter = globals()[args.adapter]()

	files = sys.stdin.read().splitlines()
	if len(files) == 0:
		sys.exit(0)

	df = pd.DataFrame(get_data(adapter.reader, files))
	# If df is empty: TypeError: Cannot infer number of levels from empty list (see above if test)
	df.columns = pd.MultiIndex.from_tuples(df.columns)

	preprocessed = adapter.preprocess(df)
	groupby_spec = adapter.groupby
	grouper = list(pd.MultiIndex.from_product([['GLOBALS'], groupby_spec]))

	# Each loop iteration generates a NcML
	for n,g in adapter.filter_fx(preprocessed).groupby(grouper):
		# Generate NcML template variables
		fxs = adapter.get_fxs(df, groupby_spec, n)
		time_values = adapter.get_time_values(g)

		# Interpolate NcML destination
		d = dict(zip(groupby_spec, n))
		name = adapter.name.format(**d)
		dest = os.path.join(args.dest, name)

		# Quality tests
		adapter.test(g, dest)

		# Generate NcML
		os.makedirs(os.path.dirname(dest), exist_ok=True)
		params = {'df': g, 'fxs': fxs, 'time_values': time_values}
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
