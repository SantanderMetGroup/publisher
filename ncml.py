#!/usr/bin/env python

import os, sys
import argparse
import pandas as pd

from ncml.adapter import Adapter

from projects.esgf.cmip6 import Cmip6NcmlAdapter
from projects.esgf.cordex import CordexNcmlAdapter
from projects.cordexEsdm.cordexEsdm import EcearthCordexEsdmNcmlAdapter, InterimCordexEsdmNcmlAdapter

def get_data(adapter, files):
	for f in files:
		try:
			metadata = adapter.read(f)
			# https://stackoverflow.com/questions/24988131/nested-dictionary-to-multiindex-dataframe-where-dictionary-keys-are-column-label
			yield {(outerKey, innerKey): value for outerKey, innerDict in metadata.items() for innerKey, value in innerDict.items()}
		except Exception as e:
			print('{},MetadataReadException,{}'.format(f, e), file=sys.stderr)

def create_dataframe(adapter):
	files = sys.stdin.read().splitlines()
	if len(files) == 0:
		sys.exit(1)

	df = pd.DataFrame(get_data(adapter, files))
	# If df is empty: TypeError: Cannot infer number of levels from empty list (see above if test)
	df.columns = pd.MultiIndex.from_tuples(df.columns)

	return df

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Read a list of files from stdin and generate NcMLs')
	parser.add_argument('--dest', dest='dest', type=str, help='Destination directory')
	parser.add_argument('--adapter', dest='adapter', type=str, help='Adapter class')
	parser.add_argument('--save-dataframe', dest='save_dataframe', type=str, default=None, help='Save pandas dataframe using pytables and exit')
	args = parser.parse_args()

	if args.adapter is None:
		adapter = Adapter(args.dest)
	else:
		adapter = globals()[args.adapter](args.dest)

	df = create_dataframe(adapter)
	if args.save_dataframe:
		store = pd.HDFStore(args.save_dataframe)
		store['df'] = df
		sys.exit(0)

	preprocessed = adapter.preprocess(df)
	# Each loop iteration generates a NcML
	for group in adapter.group(preprocessed):
		path = adapter.to_ncml(group)
		adapter.test(group, path)
		print(path)
