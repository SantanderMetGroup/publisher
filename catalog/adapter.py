import os
from datetime import datetime
from lxml import etree

class Adapter():
	def __init__(self):
		self.template = 'base.xml.j2'
		self.rtemplate = 'root.xml.j2'

	def group(self, file):
		raise NotImplementedError

	def process_dataset(self, dataset):
		"""Obtains information from a dataset, either ncml, netCDF,...

		Parameters:
		dataset (string): Full path to a dataset in the filesystem

		Returns:
		dict: Key-value properties and values from the dataset
		"""
		raise NotImplementedError

	def process_catalog(self, catalog):
		"""Obtains information from a TDS catalog to be processed by Jinja templates.

		Parameters:
		catalog (string): Full path to a TDS catalog file in the filesystem

		Returns:
		dict: Key-value properties and values from the catalog
		"""
		raise NotImplementedError

class BaseAdapter(Adapter):
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

	def process_ncml(self, ncml):
		namespaces = {'unidata': 'http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2'}
		tree = etree.parse(ncml)

		basename = os.path.basename(ncml)
		name = os.path.splitext(basename)[0]
		ext = os.path.splitext(basename)[1]
		last_modified = datetime.fromtimestamp(os.stat(ncml).st_mtime)
		size = tree.xpath('/unidata:netcdf/unidata:attribute[@name="size"]', namespaces=namespaces)[0]
		primary_variables = tree.xpath('/unidata:netcdf/unidata:attribute[@name="primary_variables"]', namespaces=namespaces)[0]

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
