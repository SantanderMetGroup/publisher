import os
import re
from jinja2 import Environment, FileSystemLoader, select_autoescape

class Adapter():
    def __init__(self, dest=None):
        if dest is None:
            self.dest = os.getcwd()
        else:
            self.dest = dest

        self.template = 'base.xml.j2'
        self.root_template = 'root.xml.j2'
        self.templates = os.path.join(os.path.dirname(__file__), 'templates')
        self.env = self.setup_jinja(self.templates)

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

    def group(self, file):
        """Given a dataset returns a string key used to classify the dataset

        Parameters:
        file (string): Full path to the dataset in the filesystem

        Returns:
        string: Key used to group the dataset
        """
        return 'catalog.xml'

    def process_dataset(self, dataset):
        """Obtains information from a dataset, either ncml, netCDF,...

        Parameters:
        dataset (string): Full path to a dataset in the filesystem

        Returns:
        dict: Key-value properties and values from the dataset
        """
        size = os.stat(dataset).st_size

        return {
            'file': dataset,
            'size': size
        }

    def process_catalog(self, catalog):
        """Obtains information from a TDS catalog to be processed by Jinja templates.

        Parameters:
        catalog (string): Full path to a TDS catalog file in the filesystem

        Returns:
        dict: Key-value properties and values from the catalog
        """
        return {
            'file': catalog
        }

    def catalog(self, catalog, datasets):
        """Given a catalog identifier and a list of datasets, creates a TDS catalog
        in the filesystem and returns its path

        Parameters:
        catalog (string): Catalog identifier as returned by group()
        datasets (list): List of dicts where each dict is a dataset as returned by process_dataset()

        Returns:
        string: Path of the catalog in the filesystem
        """
        path = os.path.join(self.dest, catalog)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        template = self.env.get_template(self.template)
        with open(path, 'w+') as fh:
            fh.write(template.render(datasets=datasets))

        return path
        
    def root_catalog(self, refs):
        """Given a list of TDS catalogs, create a TDS catalog in the filesystem
        that references all catalogs

        Parameters:
        refs (list): List of dicts where each dict is a catalog as returned by process_catalog()

        Returns:
        string: Path of the catalog in the filesystem
        """
        path = os.path.join(self.dest)
        template = self.env.get_template(self.root_template)
        with open(path, 'w+') as fh:
            fh.write(template.render(refs=refs))

        return path
