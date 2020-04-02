import os
import numpy as np
import netCDF4

class MetadataReader:
	def read(self, file):
		raise NotImplementedError

	def empty_attrs(self, variables):
		attrs = {}
		attrs['GLOBALS'] = {}

		for variable in variables:
			attrs[variable] = {}

		return attrs

class NetcdfMetadataReader(MetadataReader):
	def read(self, file):
		with netCDF4.Dataset(file) as ds:
			attrs = self.empty_attrs(ds.variables)

			for attr in ds.ncattrs():
				attrs['GLOBALS'][attr] = ds.getncattr(attr)

			for variable in ds.variables:
				for attr in ds[variable].ncattrs():
					attrs[variable][attr] = ds[variable].getncattr(attr)

			# Other metadata
			attrs['GLOBALS']['size'] = os.stat(file).st_size
			attrs['GLOBALS']['localpath'] = os.path.abspath(file)

			# Required metadata for the time variable
			if 'time' in ds.variables:
				time = ds.variables['time']
				ncoords = time.size
				value0 = time[0].data.item()
				value1 = time[1].data.item()

				attrs['time']['ncoords'] = time.size
				attrs['time']['value0'] = value0
				attrs['time']['increment'] = value1-value0

			return attrs
