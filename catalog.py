#!/usr/bin/env python

import os, sys
import argparse
from collections import defaultdict

from catalog.adapter import Adapter

# ESGF
from projects.esgf.cmip6 import Cmip6CatalogAdapter
from projects.esgf.cordex import CordexCatalogAdapter

# CordexEsdm
from projects.cordexEsdm.cordexEsdm import InterimCordexEsdmCatalogAdapter
from projects.cordexEsdm.cordexEsdm import EcearthCordexEsdmCatalogAdapter
from projects.cordexEsdm.cordexEsdm import CordexEsdmCatalogAdapter

def generate_root(adapter):
	refs = []
	for catalog in sys.stdin.read().splitlines():
		fpath = os.path.abspath(catalog)
		refs.append(adapter.process_catalog(fpath))

	path = adapter.root_catalog(refs)
	print(path)

def generate_tree(adapter):
	catalogs = defaultdict(list)

	for f in sys.stdin.read().splitlines():
		full_path = os.path.abspath(f)
		dataset = adapter.process_dataset(full_path)
		group = adapter.group(full_path)
		catalogs[group].append(dataset)

	for catalog in catalogs:
		path = adapter.catalog(catalog, catalogs[catalog])
		print(path)

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Create catalogs from datasets or catalogs from stdin.')
	parser.add_argument(
		'--dest',
		dest='dest',
		required=True,
		type=str,
		help='Destination directory.')
	parser.add_argument(
		'--adapter',
		dest='adapter',
		type=str,
		help='Adapter class.')
	parser.add_argument(
		'--root',
		dest='root',
		action='store_true',
		default=False,
		help='Generate root catalog.')
	args = parser.parse_args()

	if args.adapter is None:
		adapter = Adapter()
	else:
		adapter = globals()[args.adapter](args.dest)

	if args.root:
		generate_root(adapter)
	else:
		generate_tree(adapter)
