import os
import netCDF4
from jinja2 import Environment, FileSystemLoader, select_autoescape

class Adapter():
    def __init__(self, dest=None, groupby=None, template=None):
        if dest is None:
            self.dest = os.path.join(os.getcwd(), 'unnamed.ncml')
        else:
            self.dest = dest

        if template is None:
            self.template = 'base.ncml.j2'
        else:
            self.template = template

        self.groupby = groupby

        # initialize jinja environment
        templates = os.path.join(os.path.dirname(__file__), 'templates')
        self.env = self.setup_jinja(templates)

    def setup_jinja(self, templates):
        env = Environment(
            loader=FileSystemLoader(templates),
            autoescape=select_autoescape(['xml']),
            trim_blocks=True,
            lstrip_blocks=True)

        env.filters['basename'] = lambda path: os.path.basename(path)
        env.filters['dirname'] = lambda path: os.path.dirname(path)
        env.filters['regex_replace'] = lambda s, find, replace: re.sub(find, replace, s)

        env.tests['isncml'] = lambda dataset: dataset['ext'] == ".ncml"
        env.tests['isnc'] = lambda dataset: dataset['ext'] != ".ncml"

        return env

    def read(self, file):
        attrs = {}
        attrs['GLOBALS'] = {}
        attrs['GLOBALS']['size'] = os.stat(file).st_size
        attrs['GLOBALS']['localpath'] = os.path.abspath(file)
        return attrs

    def preprocess(self, df):
        return df

    def group(self, df):
        if self.groupby is None:
            yield df
        else:
            how_to_group = [('GLOBALS', facet) for facet in self.groupby.split(',')]
            for _,group in df.groupby(how_to_group):
                yield group

    def to_ncml(self, df):
        d = dict(df['GLOBALS'].iloc[0])
        formatted = self.dest.format(**d)
        path = os.path.abspath(formatted)
        os.makedirs(os.path.dirname(path), exist_ok=True)

        t = self.env.get_template(self.template)
        with open(path, 'w+') as fh:
            fh.write(t.render({'df': df})) 

        return path

    def test(self, df, ncml):
        pass

class NetcdfAdapter(Adapter):
    def read(self, file):
        attrs = super().read(file)
        with netCDF4.Dataset(file) as ds:
            for attr in ds.ncattrs():
                attrs['GLOBALS'][attr] = ds.getncattr(attr)

            for variable in ds.variables:
                attrs[variable] = {}
                for attr in ds[variable].ncattrs():
                    attrs[variable][attr] = ds[variable].getncattr(attr)

        return attrs
