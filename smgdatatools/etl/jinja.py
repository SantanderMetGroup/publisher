import base64
import importlib
import os
import re

import pkg_resources
from jinja2 import ChoiceLoader, FileSystemLoader, Environment, select_autoescape
from smgdatatools.model.model import Variable

from smgdatatools.etl.lib import Etl, convert_times, join_existing, calculate_chunk_idx, to_numpy

# Filters and tests
def is_dimension(variable: Variable):
    dim = False
    attrs = variable.attrs
    for attr in attrs:
        if attr.name == "NAME" and attr.value.startswith("This is a netCDF dimension but not a netCDF variable"):
            dim = True
            break

    return dim

class JinjaEtl(Etl):
    def __init__(self, template, opts):
        self.opts = opts
        self.env = self.setup_jinja(os.path.abspath(os.path.dirname(template)))
        self.template = self.env.get_template(os.path.basename(template))

    def run(self, dest, collector, stores, aggregations):
        with open(dest, "w") as fh:
            fh.write(self.template.render({
                "stores": stores,
                "collector": collector,
                "aggregations": aggregations,
                **self.opts
            }))

    @staticmethod
    def setup_jinja(templates):
        default_templates = os.path.join(os.path.dirname(__file__), "templates")

        if importlib.util.find_spec('smgdatatools') is not None:
            loader = ChoiceLoader([
                FileSystemLoader(templates),
                FileSystemLoader(os.getcwd()),
                FileSystemLoader(default_templates),
                FileSystemLoader(pkg_resources.resource_filename('smgdatatools', 'templates/')),
            ])
        else:
            loader = ChoiceLoader([
                FileSystemLoader(templates),
                FileSystemLoader(os.getcwd()),
                FileSystemLoader(default_templates),
            ])

        env = Environment(
            loader=loader,
            autoescape=select_autoescape(["xml"]),
            trim_blocks=True,
            lstrip_blocks=True)

        env.filters["regex_replace"] = lambda s, find, replace: re.sub(find, replace, s)
        env.filters["attrs_dict"] = lambda attrs: {attr.name: attr.value for attr in attrs}
        env.filters["attrs_escape"] = lambda dict_items: {"\\\"{}\\\":\\\"{}\\\"".format(x[0], x[1].replace("\"", ""))
                                                          for x in dict_items}
        env.filters["join_existing"] = join_existing
        env.filters["calculate_chunk_idx"] = calculate_chunk_idx
        env.filters["convert_times"] = convert_times
        env.filters["to_numpy"] = to_numpy
        env.filters['b64decode'] = base64.b64decode
        env.filters['b64encode'] = base64.b64encode

        env.tests["is_dimension"] = is_dimension

        return env
