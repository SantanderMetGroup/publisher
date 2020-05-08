#!/usr/bin/env python

import os, sys
import argparse
import re
from datetime import datetime

from collections import defaultdict
from jinja2 import Environment, FileSystemLoader, select_autoescape, Template

from catalog.adapter import *
from projects.cmip6 import Cmip6CatalogAdapter
from projects.cordex import CordexCatalogAdapter
from projects.cordexEsdm import InterimCordexEsdmCatalogAdapter, EcearthCordexEsdmCatalogAdapter

def generate_root(root, root_name, template, adapter):
	refs = []
	for catalog in sys.stdin.read().splitlines():
		fpath = os.path.abspath(catalog)
		refs.append(adapter.process_catalog(fpath))

	templates = os.path.join(os.path.dirname(__file__), 'templates/catalogs')
	env = Environment(loader=FileSystemLoader(templates), autoescape=select_autoescape(['xml']))
	env.filters['regex_replace'] = lambda s, find, replace: re.sub(find, replace, s)
	template = env.get_template(template)

	with open(root, 'w+') as fh:
		fh.write(template.render(name=root_name, catalogs=refs))

	print(root)

def generate(catalog, name, datasets, template):
	templates = os.path.join(os.path.dirname(__file__), 'templates/catalogs')
	env = Environment(loader=FileSystemLoader(templates), autoescape=select_autoescape(['xml']))

	env.filters['regex_replace'] = lambda s, find, replace: re.sub(find, replace, s)
	env.tests['isncml'] = lambda dataset: dataset['ext'] == ".ncml"
	env.tests['isnc'] = lambda dataset: dataset['ext'] != ".ncml"

	template = env.get_template(template)
	os.makedirs(os.path.dirname(catalog), exist_ok=True)
	with open(catalog, 'w+') as fh:
		fh.write(template.render(name=name, datasets=datasets))

	print(catalog)


def generate_tree(name, dest, adapter):
	catalogs = defaultdict(list)

	for f in sys.stdin.read().splitlines():
		dataset = adapter.process_dataset(f)

		# add dataset to corresponding catalog
		group = adapter.group(f)
		catalogs[group].append(dataset)

	# generate catalogs
	for catalog in catalogs:
		catalog_name = '_'.join([name, catalog.replace('/', '_')]) if name else catalog.replace('/', '_')
		generate(os.path.join(dest, catalog, 'catalog.xml'), catalog_name, catalogs[catalog], adapter.template)

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Create a catalog from a list of ncmls.')

	# used in tree generation and root catalog
	parser.add_argument('--name', dest='name', type=str, help='Name for root catalog or string to prepend for tree catalogs')
	parser.add_argument('--dest', dest='dest', type=str, help='Destination directory.')
	parser.add_argument('--adapter', dest='adapter', type=str, help='Adapter class.')
	# root catalog generation
	parser.add_argument('--root', dest='root', type=str, default=None, help='Generate root catalog')
	parser.add_argument('--template', dest='template', type=str, help='Template to use.')

	args = parser.parse_args()
	adapter = globals()[args.adapter]()
	if args.root:
		generate_root(args.root, args.name, args.template, adapter)
	else:
		generate_tree(args.name, args.dest, adapter)
