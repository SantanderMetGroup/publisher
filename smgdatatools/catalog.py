#!/usr/bin/env python3

import argparse
import importlib
import os
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import netCDF4
import pkg_resources
from jinja2 import Environment, FileSystemLoader, ChoiceLoader, select_autoescape


class Project():
    def __init__(self, drs, catalog):
        self.drs = drs
        if catalog is None:
            self.catalog = "catalog.xml"
        else:
            self.catalog = catalog

    def get_drs(self, item):
        drs = dict()
        if self.drs is not None:
            p = re.compile(self.drs)
            matches = p.search(item)

            drs = dict(matches.groupdict())

        return drs

    def get_attrs(self, item):
        return dict()

    def get_meta(self, item):
        meta = dict()

        meta["name"] = os.path.basename(item)
        meta["path"] = item

        st = os.stat(item)
        meta["size"] = st.st_size
        meta["last_modified"] = datetime.fromtimestamp(
            st.st_mtime,
            tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S %z")

        return meta


class Netcdf(Project):
    def get_attrs(self, item):
        ds = netCDF4.Dataset(item)
        attrs = dict()
        for attr in ds.ncattrs():
            attrs[attr] = ds.getncattr(attr)
        ds.close()

        return attrs

    def get_size(self, item):
        st = os.stat(item)
        return st.st_size


class H5vds(Netcdf):
    def get_size(self, item):
        size = 0
        ds = netCDF4.Dataset(item)
        if "size" in ds.ncattrs():
            size = ds.getncattr("size")
        ds.close()
        return size


class Ncml(Project):
    def get_attrs(self, item):
        basename = os.path.basename(item)
        st = os.stat(item)

        attrs = {
            "name": basename.rstrip(".ncml"),
            "size": self.get_size(item),
        }

        return attrs

    def get_size(self, item):
        tree = ET.parse(item)
        ET.register_namespace("", "http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2")
        size = tree.find("{http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2}" + "attribute[@name='size']")

        return size.attrib["value"]


class RootCatalog(Project):
    def get_attrs(self, item):
        tree = ET.parse(item)
        ET.register_namespace("", "http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0")
        ET.register_namespace("xlink", "http://www.w3.org/1999/xlink")

        size = 0
        for esize in tree.getroot().iter("{http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0}dataSize"):
            if esize:
                size += int(esize.text)

        return {
            "size": str(size)
        }


class Dataset():
    def __init__(self, path, attrs, drs, meta):
        self.path = path
        self.attrs = attrs
        self.drs = drs
        self.meta = meta


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

    env.filters["basename"] = lambda path: os.path.basename(path)
    env.filters["dirname"] = lambda path: os.path.dirname(path)
    env.filters["regex_replace"] = lambda s, find, replace: re.sub(find, replace, s)

    return env


if __name__ == "__main__":
    # arguments
    parser = argparse.ArgumentParser(description="Generate TDS catalogs.")
    parser.add_argument("--root", type=str, nargs="?", default=os.getcwd(),
                        help="path where the catalog will be generated.")
    parser.add_argument("--catalog", type=str, nargs="?",
                        help="fstring of the catalog.")
    parser.add_argument("--drs", type=str, nargs="?",
                        help="regex DRS of the input datasets.")
    parser.add_argument("--template", type=str, nargs="?", default="catalog.xml.j2",
                        help="etl template of the catalog.")
    parser.add_argument("--project", choices=["Ncml", "Netcdf", "H5vds", "RootCatalog"],
                        help="project configuration.")
    parser.add_argument("--params", type=str, nargs="*", default=list(),
                        help="additional key=value params for the etl template.")
    args = vars(parser.parse_args())

    if args["project"] == "Ncml":
        project = Ncml(args["drs"], args["catalog"])
    elif args["project"] == "RootCatalog":
        project = RootCatalog(args["drs"], args["catalog"])
    elif args["project"] == "Netcdf":
        project = Netcdf(args["drs"], args["catalog"])
    elif args["project"] == "H5vds":
        project = H5vds(args["drs"], args["catalog"])
    else:
        project = Project(args["drs"], args["catalog"])

    # etl
    t = args["template"]
    template_abs_path = os.path.abspath(t)
    env = setup_jinja(os.path.dirname(template_abs_path))
    template = env.get_template(os.path.basename(t))

    # process datasets
    datasets = list()
    attrs = dict()
    drs = dict()
    for i in sys.stdin:
        path = i.rstrip("\n")
        drs = project.get_drs(path)
        attrs = project.get_attrs(path)
        meta = project.get_meta(path)

        dataset = Dataset(path, attrs, drs, meta)
        datasets.append(dataset)

    # render
    attrs.update(drs)
    catname = project.catalog.format(**attrs)
    catpath = os.path.join(args["root"], catname)
    os.makedirs(os.path.dirname(catpath), exist_ok=True)

    catalog = {
        "name": catname,
        "path": catpath,
    }

    with open(catpath, "w") as fh:
        params = {"catalog": catalog, "datasets": datasets}
        for p in args["params"]:
            k = p.split("=")[0]
            v = "=".join(p.split("=")[1:])
            params[k] = v

        fh.write(template.render(params))

    print(catpath)
    sys.exit(0)
