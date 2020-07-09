# https://gitlab.com/scds/tds-content/-/issues/4

import os
import re
import pandas as pd
import netCDF4
from lxml import etree
from datetime import datetime
from jinja2 import Environment, FileSystemLoader, select_autoescape

from ncml.adapter import NetcdfAdapter as NcmlAdapter
from catalog.adapter import Adapter as CatalogAdapter

class CordexEsdmNcmlAdapter(NcmlAdapter):
    def __init__(self, dest):
        self.dest = dest
        self.template = 'cordexEsdm.ncml.j2'

        templates = os.path.join(os.path.dirname(__file__), 'templates')
        self.env = self.setup_jinja(templates)

    def read(self, file):
        attrs = super().read(file)

        with netCDF4.Dataset(file) as ds:
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

class EcearthCordexEsdmNcmlAdapter(CordexEsdmNcmlAdapter):
    def __init__(self, dest):
        super().__init__(dest)
        self.groupby = 'project,model,run,experiment,domain'
        self.filename = '{project}_{model}_{run}_{experiment}_{domain}.ncml'
        self.dest = os.path.join(self.dest, self.filename)

    def read(self, file):
        # nc example: hus@1000_CMIP5_EC-EARTH_r12i1p1_rcp85_EUR.nc4
        attrs = super().read(file)
        basename = os.path.splitext(os.path.basename(file))[0]
        facets = basename.split('_')

        attrs['GLOBALS']['variable'] = facets[0].replace('@', '')
        attrs['GLOBALS']['project'] = facets[1]
        attrs['GLOBALS']['model'] = facets[2]
        attrs['GLOBALS']['run'] = facets[3]
        attrs['GLOBALS']['experiment'] = facets[4]
        attrs['GLOBALS']['domain'] = facets[5]

        return attrs

class InterimCordexEsdmNcmlAdapter(CordexEsdmNcmlAdapter):
    def __init__(self, dest):
        super().__init__(dest)
        self.groupby = 'institution,model,domain'
        self.filename = '{institution}_{model}_{domain}.ncml'
        self.dest = os.path.join(self.dest, self.filename)

    def read(self, file):
        # nc example: ta@1000_ECMWF_ERA-Interim-ESD_EUR.nc4
        attrs = super().read(file)
        basename = os.path.splitext(os.path.basename(file))[0]
        facets = basename.split('_')

        attrs['GLOBALS']['variable'] = facets[0].replace('@', '')
        attrs['GLOBALS']['institution'] = facets[1]
        attrs['GLOBALS']['model'] = facets[2]
        attrs['GLOBALS']['domain'] = facets[3]

        return attrs

class CordexEsdmCatalogAdapter(CatalogAdapter):
    def __init__(self, dest):
        self.template = 'cordexEsdm.xml.j2'
        self.root_template = 'root.xml.j2'
        self.dest = dest

        templates = os.path.join(os.path.dirname(__file__), 'templates')
        self.env = self.setup_jinja(templates)

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


    def process_dataset(self, dataset):
        ext = os.path.splitext(dataset)[1]
        if ext == ".ncml":
            d = self.process_ncml(dataset)
        else:
            d = self.process_nc(dataset)

        return d

    def process_ncml(self, ncml):
        namespaces = {'unidata': 'http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2'}
        tree = etree.parse(ncml)

        basename = os.path.basename(ncml)
        name = os.path.splitext(basename)[0]
        ext = os.path.splitext(basename)[1]
        last_modified = datetime.fromtimestamp(os.stat(ncml).st_mtime)
        size = tree.xpath(
            '/unidata:netcdf/unidata:attribute[@name="size"]',
            namespaces=namespaces)[0]
        primary_variables = tree.xpath(
            '/unidata:netcdf/unidata:attribute[@name="primary_variables"]',
            namespaces=namespaces)[0]

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

class InterimCordexEsdmCatalogAdapter(CordexEsdmCatalogAdapter):
    def group(self, file):
        basename = os.path.basename(file)
        name, ext = os.path.splitext(basename)
        facets = name.split('_')

        if ext == ".ncml":
            grouper = [facets[i] for i in [1,2]]
        else:
            grouper = [facets[i] for i in [2,3]]

        return os.path.join(*grouper, 'catalog.xml')

class EcearthCordexEsdmCatalogAdapter(CordexEsdmCatalogAdapter):
    def group(self, file):
        basename = os.path.basename(file)
        name, ext = os.path.splitext(basename)
        facets = name.split('_')

        if ext == ".ncml":
            grouper = [facets[i] for i in [1,2,4,3]]
        else:
            grouper = [facets[i] for i in [2,3,5,4]]

        return os.path.join(*grouper, 'catalog.xml')
