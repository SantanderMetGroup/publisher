import re
import sys, os
import numpy as np
import pandas as pd
import netCDF4, cftime
from datetime import datetime
from lxml import etree
from jinja2 import Environment, FileSystemLoader, select_autoescape

from ncml.adapter import Adapter as NcmlAdapter
from catalog.adapter import Adapter as CatalogAdapter

class EsgfCatalogAdapter(CatalogAdapter):
	def __init__(self):
		# initialize jinja environment
		self.templates = os.path.join(os.path.dirname(__file__), 'templates')
		self.env = Environment(
			loader=FileSystemLoader(self.templates),
			autoescape=select_autoescape(['xml']))

		self.env.filters['regex_replace'] = lambda s, find, replace: re.sub(find, replace, s)
		self.env.tests['isncml'] = lambda dataset: dataset['ext'] == ".ncml"
		self.env.tests['isnc'] = lambda dataset: dataset['ext'] != ".ncml"

	def process_dataset(self, dataset):
		ext = os.path.splitext(dataset)[1]
		if ext == ".ncml":
			d = self.process_ncml(dataset)
		else:
			d = self.process_nc(dataset)

		return d

	def process_catalog(self, catalog):
		tree = etree.parse(catalog)
		namespaces = {'unidata': 'http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0'}
		name = tree.xpath('/unidata:catalog/@name', namespaces=namespaces)[0]

		return {
            'file': catalog,
			'title': name,
			'size': self.catalog_size(catalog),
			'last_modified': datetime.fromtimestamp(os.stat(catalog).st_mtime)
		}

	def catalog(self, catalog, datasets):
		path = os.path.join(self.dest, catalog, 'catalog.xml')
		template = self.env.get_template(self.template)
		os.makedirs(os.path.dirname(path), exist_ok=True)
		with open(path, 'w+') as fh:
			fh.write(template.render(name=catalog, datasets=datasets, namespace=self.namespace))

		return path

	def root_catalog(self, refs):
		path = self.dest
		template = self.env.get_template(self.root_template)

		with open(path, 'w+') as fh:
			fh.write(template.render(catalogs=refs, namespace=self.namespace))

		return path

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
		primary_variables = tree.xpath(
			'/unidata:netcdf/unidata:attribute[@name="primary_variables"]',
			namespaces=namespaces)[0]

		return {
			'file': ncml,
			'name': name,
			'last_modified': last_modified,
			'service': 'virtual',
			'ext': ext,
			'size': int(size.attrib['value']),
			'primary_variables': primary_variables.attrib['value']
		}

	def process_nc(self, nc):
		basename = os.path.basename(nc)
		name = os.path.splitext(basename)[0]
		ext = os.path.splitext(basename)[1]
		last_modified = datetime.fromtimestamp(os.stat(nc).st_mtime)
		size = os.stat(nc).st_size

		return {
			'file': nc,
			'name': name,
			'last_modified': last_modified,
			'size': size,
			'service': 'all',
			'ext': ext
		}

	def catalog_size(self, catalog):
		namespaces = {'unidata': 'http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0'}
		tree = etree.parse(catalog)
		sizes = tree.xpath('//unidata:dataSize', namespaces=namespaces)

		total_size = 0
		for s in sizes:
			total_size += int(s.text)

		return total_size

class EsgfNcmlAdapter(NcmlAdapter):
	def read(self, file):
		attrs = super().read(file)
		dirname = os.path.dirname(file)
		attrs['GLOBALS']['version'] = dirname.split('/')[-1]

		with netCDF4.Dataset(file) as ds:
			# Required metadata for the time variable
			if 'time' in ds.variables:
				time = ds.variables['time']
				ncoords = time.size
				value0 = time[0].data.item()
				value1 = time[1].data.item()
	
				attrs['time']['ncoords'] = time.size
				attrs['time']['value0'] = value0
				attrs['time']['increment'] = value1-value0
	
				# check if time is regular within the file
				u = np.unique(np.diff(ds.variables['time']))
				attrs['time']['regular'] = u.size==1

		return attrs

	def test(self, df, ncml):
		for variable in df[df[('GLOBALS', 'frequency')] != 'fx'][('GLOBALS', 'variable_id')].unique():
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
