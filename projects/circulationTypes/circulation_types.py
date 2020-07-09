import os

from projects.esgf.cmip6 import Cmip6NcmlAdapter
from projects.esgf.cmip5 import Cmip5NcmlAdapter
from projects.esgf.esgf import EsgfCatalogAdapter

class CirculationTypesCmip6NcmlAdapter(Cmip6NcmlAdapter):
    def __init__(self, dest):
        super().__init__(dest)
        self.path = os.path.join('circulation-types', 'cmip6', self.filename)

class CirculationTypesCmip5NcmlAdapter(Cmip5NcmlAdapter):
    def __init__(self, dest):
        super().__init__(dest)
        self.path = os.path.join('circulation-types', 'cmip5', self.filename)

class CirculationTypesCatalogAdapter(EsgfCatalogAdapter):
    def __init__(self, dest):
        super().__init__(dest)
        self.template = 'catalog.xml.j2'
        self.namespace = 'devel/circulation-types'

        # Need to override location of templates
        templates = os.path.join(os.path.dirname(__file__), 'templates')
        self.env = self.setup_jinja(templates)
