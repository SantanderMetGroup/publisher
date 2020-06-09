import os
from datetime import datetime
from lxml import etree

class Adapter():
	def __init__(self):
		self.template = 'base.xml.j2'
		self.rtemplate = 'root.xml.j2'

	def group(self, file):
		"""Given a dataset returns a string key used to classify the dataset

		Parameters:
		file (string): Full path to the dataset in the filesystem

		Returns:
		string: Key used to group the dataset
		"""
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

	def catalog(self, catalog, datasets):
		"""Given a catalog identifier and a list of datasets, creates a TDS catalog
		in the filesystem and returns its path

		Parameters:
		catalog (string): Catalog identifier as returned by group()
		datasets (list): List of dicts where each dict is a dataset as returned by process_dataset()

		Returns:
		string: Path of the catalog in the filesystem
		"""
		raise NotImplementedError

	def root_catalog(self, refs):
		"""Given a list of TDS catalogs, create a TDS catalog in the filesystem
		that references all catalogs

		Parameters:
		refs (list): List of dicts where each dict is a catalog as returned by process_catalog()

		Returns:
		string: Path of the catalog in the filesystem
		"""
		raise NotImplementedError
