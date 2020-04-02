import os

class Adapter():
	def __init__(self):
		self.template = 'base.xml.j2'
		self.rtemplate = 'root.xml.j2'

	def group(self, file):
		raise NotImplementedError
