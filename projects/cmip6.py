import os, sys
import re
import pandas as pd
import netCDF4
from datetime import datetime

from ncml.adapter import EsgfNcmlAdapter
from ncml.reader import EsgfMetadataReader
from catalog.adapter import Adapter as CatalogAdapter

class Cmip6CatalogAdapter(CatalogAdapter):
	def __init__(self):
		self.template = 'cmip6/cmip6-noextension.xml.j2'
		#self.template = 'cmip6/cmip6.xml.j2'
		self.rtemplate = 'cmip6/root.xml.j2'

	def group(self, file):
		basename = os.path.basename(file)
		name = os.path.splitext(basename)[0]
		facets = name.split('_')
		grouper = [facets[i] for i in [0,1,2,3,4,6]]

		return '/'.join(grouper)

class Cmip6MetadataReader(EsgfMetadataReader):
	def read(self, file):
		attrs = super().read(file)
		return attrs

class Cmip6NcmlAdapter(EsgfNcmlAdapter):
	def __init__(self):
		self.reader = Cmip6MetadataReader()

		self.directory = 'cmip6/{activity_id}/{institution_id}/{source_id}/{experiment_id}/{table_id}'
		self.filename = 'cmip6_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{variant_label}_{table_id}.ncml'
		self.name = os.path.join(self.directory, self.filename)

		self.template = 'cmip6/cmip6.ncml.j2'
		self.groupby = ['mip_era', 'activity_id','source_id' ,'institution_id' ,'experiment_id' ,'variant_label' , 'realm', 'table_id', 'frequency']

		self.fxs = ['areacella', 'areacellr', 'orog', 'sftlf', 'sftgif', 'mrsofc', 'rootd', 'zfull']
		self.fxs_facets = ['mip_era', 'activity_id','source_id' ,'institution_id' ,'experiment_id' ,'variant_label', 'realm']

	def preprocess(self, df):
		preprocessed = super().preprocess(df)

		# Fix AerChemMIP activity_id attribute
		i = ('GLOBALS', 'activity_id')
		preprocessed.loc[:, i] = preprocessed.loc[:, i].str.replace('ScenarioMIP AerChemMIP', 'ScenarioMIP')

		return preprocessed
