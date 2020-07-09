import os
import re
import pandas as pd

from projects.esgf.esgf import EsgfNcmlAdapter, EsgfCatalogAdapter

class Cmip5CatalogAdapter(EsgfCatalogAdapter):
    def __init__(self, dest):
        super().__init__(dest)
        self.template = 'cmip5.xml.j2'
        self.root_template = 'root.xml.j2'

    def group(self, file):
        basename = os.path.basename(file)
        name = os.path.splitext(basename)[0]
        facets = name.split('_')
        grouper = [facets[i] for i in [0,1,2,3,4,6,7]]

        return os.path.join(*grouper, 'catalog.xml')

class Cmip5NcmlAdapter(EsgfNcmlAdapter):
    def __init__(self, dest):
        super().__init__()
        self.template = 'cmip5.ncml.j2'
        self.directory = 'CMIP5/{product}/{institute_id}/{model_id}/{experiment_id}/{frequency}/{modeling_realm}/{table_id}'
        self.filename = 'CMIP5_{product}_{institute_id}_{model_id}_{experiment_id}_{frequency}_{modeling_realm}_{table_id}_{parent_experiment_rip}.ncml'
        self.dest = os.path.join(dest, self.directory, self.filename)

        self.group_time = ['project_id', 'product','institute_id' ,'model_id' ,'experiment_id' ,'frequency' , 'modeling_realm', 'table_id', 'parent_experiment_rip']
        self.group_fx = ['project_id', 'product','institute_id' ,'model_id' ,'experiment_id' , 'modeling_realm']
        self.group_latest_versions = ['project_id', 'product','institute_id' ,'model_id' ,'experiment_id' ,'frequency' , 'modeling_realm', 'table_id', 'parent_experiment_rip']

    def read(self, file):
        # cmip5 netcdf attributes suck, better to use drs but...
        # drs also sucks (sometimes variable is part of drs sometimes it's not)
        attrs = super().read(file)
        dirname = os.path.dirname(file)
        filename = os.path.basename(file)
        facets = dirname.split('/')

        version_pattern = re.compile("v[0-9]{6}")
        if version_pattern.match(facets[-1]):
            attrs['GLOBALS']['variable_id'] = filename.split('_')[0]
            attrs['GLOBALS']['version'] = facets[-1]
            attrs['GLOBALS']['parent_experiment_rip'] = facets[-2]
            attrs['GLOBALS']['table_id'] = facets[-3]
        else:
            attrs['GLOBALS']['variable_id'] = facets[-1]
            attrs['GLOBALS']['version'] = facets[-2]
            attrs['GLOBALS']['parent_experiment_rip'] = facets[-3]
            attrs['GLOBALS']['table_id'] = facets[-4]
            
        return attrs
