import re
import sys, os
import pandas as pd
import netCDF4, cftime
from datetime import datetime

from ncml.reader import *

class Adapter():
	def __init__(self, name, template, groupby):
		self.name = name
		self.template = template
		self.groupby = groupby.split(',')
		self.reader = NetcdfMetadataReader()

	def filter_fx(self, df):
		return df

	def get_fxs(self, df, facets, values):
		return []

	def get_time_values(self, df):
		return []

	def preprocess(self, df):
		return df

	def test(self, df, ncml):
		pass
