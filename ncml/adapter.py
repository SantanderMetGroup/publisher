import os
import netCDF4
from jinja2 import Environment, FileSystemLoader, select_autoescape

class Adapter():
	def __init__(self, dest):
		self.dest = dest
		self.template = 'base.ncml.j2'

		# initialize jinja environment
		self.templates = os.path.join(os.path.dirname(__file__), 'templates')
		self.env = Environment(
			loader=FileSystemLoader(self.templates),
			autoescape=select_autoescape(['xml']))

		self.env.filters['regex_replace'] = lambda s, find, replace: re.sub(find, replace, s)
		self.env.tests['isncml'] = lambda dataset: dataset['ext'] == ".ncml"
		self.env.tests['isnc'] = lambda dataset: dataset['ext'] != ".ncml"

	def empty_attrs(self, variables):
		attrs = {}
		attrs['GLOBALS'] = {}

		for variable in variables:
			attrs[variable] = {}

		return attrs

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

		return attrs

	def preprocess(self, df):
		return df

	def group(self, df):
		return [df]

	def to_ncml(self, df):
		path = os.path.abspath(self.dest)
		t = self.env.get_template(self.template)
		with open(path, 'w+') as fh:
			fh.write(t.render({'df': df})) 

		return path

	def test(self, df, ncml):
		pass
