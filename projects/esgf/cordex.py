import os, sys
import re
import pandas as pd
import netCDF4
from lxml import etree
from datetime import datetime

from projects.esgf.esgf import EsgfNcmlAdapter
from projects.esgf.esgf import EsgfCatalogAdapter

class CordexCatalogAdapter(EsgfCatalogAdapter):
    def __init__(self, dest):
        super().__init__(dest)
        self.namespace = 'devel/c3s34d/CORDEX'
        self.location = 'content/cordex'
        self.template = 'cordex.xml.j2'
        self.root_template = 'root.xml.j2'

    def group(self, file):
        basename = os.path.basename(file)
        name = os.path.splitext(basename)[0]
        facets = name.split('_')
        grouper = [facets[i] for i in [0,1,2,3,4,6,7,8]]

        return os.path.join(*grouper, 'catalog.xml')

    # to be removed, it's here because right now cordex ncml contains no primary_variables attribute
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

        return {
            'file': ncml,
            'name': name,
            'last_modified': last_modified,
            'service': 'virtual',
            'ext': ext,
            'size': int(size.attrib['value'])
        }

class CordexNcmlAdapter(EsgfNcmlAdapter):
    def __init__(self, dest):
        super().__init__()
        self.dest = dest
        self.template = 'cordex.ncml.j2'
        self.directory = 'CORDEX/{product}/{CORDEX_domain}/{institution_id}/{driving_model_id}/{experiment_id}/{model_id}/{rcm_version_id}/{frequency}'
        self.filename = 'CORDEX_{product}_{CORDEX_domain}_{driving_model_id}_{experiment_id}_{driving_model_ensemble_member}_{model_id}_{rcm_version_id}_{frequency}.ncml'
        self.path = os.path.join(self.directory, self.filename)

        self.group_time = ['project_id', 'product', 'CORDEX_domain', 'institution_id', 'driving_model_id', 'experiment_id', 'driving_model_ensemble_member', 'model_id', 'rcm_version_id', 'frequency']
        self.group_fx = ['project_id', 'product', 'CORDEX_domain', 'institution_id', 'driving_model_id', 'experiment_id', 'model_id', 'rcm_version_id']
        self.group_latest_versions = ['project_id', 'product', 'CORDEX_domain', 'institution_id', 'driving_model_id', 'experiment_id', 'driving_model_ensemble_member', 'model_id', 'rcm_version_id', 'frequency', 'variable_id']

    def read(self, file):
        attrs = super().read(file)
        dirname = os.path.dirname(file)
        facets = dirname.split('/')
        attrs['GLOBALS']['variable_id'] = facets[-2]
        attrs['GLOBALS']['institution_id'] = facets[-9]
        return attrs

    def preprocess(self, df):
        preprocessed = super().preprocess(df)
        # Some datasets use 'fixed' instead of 'fx' for frequency
        i = ('GLOBALS', 'frequency')
        preprocessed.loc[:, i] = preprocessed.loc[:, i].str.replace('fixed', 'fx')

        # Remove time zone from time:units
        i = ('time', 'units')
        preprocessed.loc[:, i] = preprocessed.loc[:, i].str.replace(' UTC', '')

        # Add institute_id to RCMModelName (institute_id-model_id)
        for r in preprocessed.index:
            if preprocessed.loc[r, ('GLOBALS', 'institute_id')] not in preprocessed.loc[r, ('GLOBALS', 'model_id')]:
                preprocessed.loc[r, ('GLOBALS', 'model_id')] = \
                '-'.join([preprocessed.loc[r, ('GLOBALS', 'institute_id')], preprocessed.loc[r, ('GLOBALS', 'model_id')]])

        return preprocessed
