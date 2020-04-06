import os, sys
import re
import pandas as pd
import netCDF4
from datetime import datetime

from ncml.adapter import Adapter as EsgfNcmlAdapter
from ncml.reader import EsgfMetadataReader
from catalog.adapter import Adapter as CatalogAdapter

class Cmip6CatalogAdapter(CatalogAdapter):
	def __init__(self):
		self.template = 'cmip6/cmip6.xml.j2'
		self.rtemplate = 'cmip6/root.xml.j2'

	def group(self, file):
		basename = os.path.basename(file)
		name = os.path.splitext(basename)[0]
		facets = name.split('_')
		grouper = [facets[i] for i in [0,1,2,3,4,6,7,8,9]]

		return '/'.join(grouper)

class Cmip6MetadataReader(EsgfMetadataReader):
	#drs = ['project','product','institution','model','experiment','ensemble','frequency','variable','grid','version']

	def read(self, file):
		attrs = super().read(file)
		return attrs

class Cmip6NcmlAdapter(EsgfNcmlAdapter):
	def __init__(self):
		self.reader = Cmip6MetadataReader()

		self.directory = '{mip_era}/{activity_id}/{source_id}/{institution_id}/{experiment_id}/{grid_label}/{realm}/{table_id}/{frequency}'
		self.filename = '{mip_era}_{activity_id}_{source_id}_{institution_id}_{experiment_id}_{variant_label}_{grid_label}_{realm}_{table_id}_{frequency}.ncml'
		self.name = os.path.join(self.directory, self.filename)

		self.template = 'cmip6/cmip6.ncml.j2'
		self.groupby = ['mip_era', 'activity_id','source_id' ,'institution_id' ,'experiment_id' ,'variant_label' ,'grid_label', 'realm', 'table_id', 'frequency']

		self.fxs = ['areacella', 'areacellr', 'orog', 'sftlf', 'sftgif', 'mrsofc', 'rootd', 'zfull']
		self.fxs_facets = ['mip_era', 'activity_id','source_id' ,'institution_id' ,'experiment_id' ,'variant_label' ,'grid_label', 'realm']
