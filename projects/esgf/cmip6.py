import os, sys
import re
import pandas as pd

from projects.esgf.esgf import EsgfNcmlAdapter
from projects.esgf.esgf import EsgfCatalogAdapter

class Cmip6CatalogAdapter(EsgfCatalogAdapter):
    def __init__(self, dest):
        super().__init__(dest)
        self.namespace = 'devel/atlas/cmip6'
        self.template = 'cmip6.xml.j2'
        self.root_name = 'ATLAS CMIP6 catalog'
        self.root_template = 'root.xml.j2'

    def group(self, file):
        basename = os.path.basename(file)
        name = os.path.splitext(basename)[0]
        facets = name.split('_')
        grouper = [facets[i] for i in [0,1,2,3,4,6]]

        return os.path.join(*grouper, 'catalog.xml')

class Cmip6NcmlAdapter(EsgfNcmlAdapter):
    def __init__(self, dest):
        super().__init__()
        self.template = 'cmip6.ncml.j2'
        self.directory = 'CMIP6/{activity_id}/{institution_id}/{source_id}/{experiment_id}/{table_id}'
        self.filename = 'CMIP6_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{variant_label}_{table_id}.ncml'
        self.dest = os.path.join(dest, self.directory, self.filename)

        self.group_time = (['mip_era', 'activity_id','source_id' ,'institution_id' ,
                            'experiment_id' ,'variant_label' , 'realm', 'table_id',
                            'frequency', 'grid_label'])
        self.group_fx = (['mip_era', 'activity_id','source_id' ,'institution_id' ,
                          'experiment_id' ,'variant_label', 'realm'])
        self.group_latest_versions = (['mip_era', 'activity_id','source_id' ,'institution_id',
                                       'experiment_id' ,'variant_label' , 'realm', 'table_id',
                                       'frequency', 'grid_label', 'variable_id'])

    def preprocess(self, df):
        preprocessed = super().preprocess(df)
        preprocessed = self.filter_grid_labels(df)
        # Fix AerChemMIP activity_id attribute
        i = ('GLOBALS', 'activity_id')
        preprocessed.loc[:, i] = preprocessed.loc[:, i].str.replace('ScenarioMIP AerChemMIP', 'ScenarioMIP')

        return preprocessed

    def filter_grid_labels(self, df):
        def gridlabel_to_int(grid_label):
            if grid_label == "gn":
                return 0
            elif grid_label == "gr":
                return 1
            else:
                # priority gn > gr > gr1 > gr2 > ...., 0 is greatest priority
                return int(re.sub("[^0-9]", "", grid_label)) + 1

        df[('GLOBALS', 'ngrid_label')] = df[('GLOBALS', 'grid_label')].apply(gridlabel_to_int)
        unique_grid_labels = []
        facets = ['mip_era', 'activity_id','source_id' ,'institution_id' ,'experiment_id' ,'variant_label' , 'realm', 'table_id', 'frequency']
        how_to_group = [('GLOBALS', f) for f in facets]

        for _,group in df.groupby(how_to_group):
            unique_grid_labels.append(group.nlargest(1, ('GLOBALS', 'ngrid_label'), keep='all'))

        return pd.concat(unique_grid_labels)
