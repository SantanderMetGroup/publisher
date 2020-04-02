#!/usr/bin/env python

import os, sys
import argparse
import re
from datetime import datetime

from lxml import etree
from collections import defaultdict
from jinja2 import Environment, FileSystemLoader, select_autoescape, Template

from catalog.adapter import *
from projects.cmip6 import Cmip6CatalogAdapter
from projects.cordexEsdm import InterimCordexEsdmCatalogAdapter, EcearthCordexEsdmCatalogAdapter

def ncml_size(ncml):
	namespaces = {'unidata': 'http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2'}
	tree = etree.parse(ncml)
	size = tree.xpath('/unidata:netcdf/unidata:attribute[@name="size"]', namespaces=namespaces)[0]

	return int(size.attrib['value'])

def catalog_size(catalog):
	namespaces = {'unidata': 'http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0'}
	tree = etree.parse(catalog)
	sizes = tree.xpath('//unidata:dataSize', namespaces=namespaces)

	total_size = 0
	for s in sizes:
		total_size += int(s.text)

	return total_size

def generate_root(root, root_name, template):
	refs = []
	fpath = os.path.abspath(root)
	dirname = os.path.dirname(fpath)
	namespaces = {'unidata': 'http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0'}
	for catalog in sys.stdin.read().splitlines():
		tree = etree.parse(catalog)
		name = tree.xpath('/unidata:catalog/@name', namespaces=namespaces)[0]
		href = os.path.abspath(catalog).replace(dirname, '').lstrip('/')
		refs.append({
			'title': name,
			'href': href,
			'size': catalog_size(catalog),
			'last_modified': datetime.fromtimestamp(os.stat(catalog).st_mtime)
		})

	templates = os.path.join(os.path.dirname(__file__), 'templates/catalogs')
	env = Environment(loader=FileSystemLoader(templates), autoescape=select_autoescape(['xml']))
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


def generate_tree(args):
	catalogs = defaultdict(list)
	adapter = globals()[args.adapter]()

	for f in sys.stdin.read().splitlines():
		basename = os.path.basename(f)
		name = os.path.splitext(basename)[0]
		ext = os.path.splitext(basename)[1]
		last_modified = datetime.fromtimestamp(os.stat(f).st_mtime)
		facets = name.split('_')

		if ext == ".ncml":
			size = ncml_size(f)
			service = "virtual"
		else:
			size = os.stat(f).st_size
			service = "all"

		# add dataset to corresponding catalog
		group = adapter.group(f)
		catalogs[group].append({
			'file': f,
			'name': name,
			'last_modified': last_modified,
			'size': size,
			'service': service,
			'ext': ext
		})

	# generate catalogs
	for catalog in catalogs:
		catalog_name = '_'.join([args.name, catalog.replace('/', '_')]) if args.name else catalog.replace('/', '_')
		generate(os.path.join(args.dest, catalog, 'catalog.xml'), catalog_name, catalogs[catalog], args.template)

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Create a catalog from a list of ncmls.')

	# used in tree generation and root catalog
	parser.add_argument('--name', dest='name', type=str, help='Name for root catalog or string to prepend for tree catalogs')

	# tree catalog generation
	parser.add_argument('--dest', dest='dest', type=str, help='Destination directory.')
	parser.add_argument('--template', dest='template', default='base.xml.j2', type=str, help='Template to use.')
	parser.add_argument('--adapter', dest='adapter', default='Adapter', type=str, help='Adapter class.')
	# root catalog generation
	parser.add_argument('--root', dest='root', type=str, default=None, help='Generate root catalog')

	args = parser.parse_args()
	if args.root:
		generate_root(args.root, args.name, args.template)
	else:
		generate_tree(args)
