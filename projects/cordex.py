import os, sys
import re
import pandas as pd
import netCDF4
from datetime import datetime

from ncml.adapter import EsgfNcmlAdapter
from ncml.reader import EsgfMetadataReader
from catalog.adapter import Adapter as CatalogAdapter

class CordexCatalogAdapter(CatalogAdapter):
	def __init__(self):
		self.template = 'cordex/cordex.xml.j2'
		self.rtemplate = 'cordex/root.xml.j2'

	def group(self, file):
		basename = os.path.basename(file)
		name = os.path.splitext(basename)[0]
		facets = name.split('_')
		grouper = [facets[i] for i in [0,1,2,3,4,5,7,8,9]]

		return '/'.join(grouper)

class CordexMetadataReader(EsgfMetadataReader):
	# drs=['activity', 'product', 'domain', 'institution', 'gcmmodelname', 'cmip5experimentname', 'cmip5ensemblemenber', 'rcmmodelname', 'rcmversionid', 'frequency', 'variable', 'version']
	def read(self, file):
		attrs = super().read(file)
		dirname = os.path.dirname(file)
		facets = dirname.split('/')
		attrs['GLOBALS']['variable_id'] = facets[-2]
		attrs['GLOBALS']['institution_id'] = facets[-9]
		return attrs

class CordexNcmlAdapter(EsgfNcmlAdapter):
	def __init__(self):
		self.reader = CordexMetadataReader()

		self.directory = '{project_id}/{product}/{CORDEX_domain}/{institution_id}/{driving_model_id}/{experiment_id}/{model_id}/{rcm_version_id}/{frequency}'
		self.filename = '{project_id}_{product}_{CORDEX_domain}_{institution_id}_{driving_model_id}_{experiment_id}_{driving_model_ensemble_member}_{model_id}_{rcm_version_id}_{frequency}.ncml'
		self.name = os.path.join(self.directory, self.filename)

		self.template = 'cordex/cordex.ncml.j2'
		self.groupby = ['project_id', 'product', 'CORDEX_domain', 'institution_id', 'driving_model_id', 'experiment_id', 'driving_model_ensemble_member', 'model_id', 'rcm_version_id', 'frequency']

		self.fxs = ['areacella', 'areacellr', 'orog', 'sftlf', 'sftgif', 'mrsofc', 'rootd', 'zfull']
		self.fxs_facets = ['project_id', 'product', 'CORDEX_domain', 'institution_id', 'driving_model_id', 'experiment_id', 'model_id', 'rcm_version_id']
