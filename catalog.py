#!/usr/bin/env python

import os, sys
from collections import defaultdict

from catalog.adapter import Adapter

# ESGF
from projects.esgf.cmip6 import Cmip6CatalogAdapter
from projects.esgf.cmip5 import Cmip5CatalogAdapter
from projects.esgf.cordex import CordexCatalogAdapter

# CordexEsdm
from projects.cordexEsdm.cordexEsdm import InterimCordexEsdmCatalogAdapter
from projects.cordexEsdm.cordexEsdm import EcearthCordexEsdmCatalogAdapter
from projects.cordexEsdm.cordexEsdm import CordexEsdmCatalogAdapter

# Circulation Types
from projects.circulationTypes.circulation_types import CirculationTypesCatalogAdapter

_help = '''Usage:
    python catalog.py [--adapter ADAPTER] [--root]

Options:
    -h, --help                  Show this message.
    --adapter ADAPTER           Use ADAPTER instead of Adapter from 'catalog/adapter.py'
    --root                      Lines from stdin are not datasets but catalogs.

Base Adapter options:
    --dest DIRECTORY            Save catalog into DIRECTORY
'''

def generate_root(adapter):
    refs = []
    for catalog in sys.stdin.read().splitlines():
        fpath = os.path.abspath(catalog)
        refs.append(adapter.process_catalog(fpath))

    path = adapter.root_catalog(refs)
    print(path)

def generate_tree(adapter):
    catalogs = defaultdict(list)

    for f in sys.stdin.read().splitlines():
        full_path = os.path.abspath(f)
        dataset = adapter.process_dataset(full_path)
        group = adapter.group(full_path)
        catalogs[group].append(dataset)

    for catalog in catalogs:
        path = adapter.catalog(catalog, catalogs[catalog])
        print(path)

if __name__ == '__main__':
    # Parse arguments
    adapter, root = None, False
    adapter_opts = {}
    position = 1
    arguments = len(sys.argv) - 1
    while arguments >= position:
        if sys.argv[position] == '-h' or sys.argv[position] == '--help':
            print(_help)
            sys.exit(0)
        elif sys.argv[position] == '--adapter':
            adapter = sys.argv[position+1]
            position += 2
        elif sys.argv[position] == '--root':
            root = True
            position += 1
        else: # Adapter arguments
            opt = sys.argv[position].lstrip('-')
            adapter_opts[opt] = sys.argv[position+1]
            position += 2

    if adapter is None:
        adapter = Adapter(**adapter_opts)
    else:
        adapter = globals()[adapter](**adapter_opts)

    if root:
        generate_root(adapter)
    else:
        generate_tree(adapter)
