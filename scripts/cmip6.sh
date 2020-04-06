#!/bin/bash

< circulation-types-inventory python ncml.py --adapter Cmip6NcmlAdapter --dest circulation-types/ncmls
find circulation-types/ncmls/ -type f | python catalog.py --adapter Cmip6CatalogAdapter --dest circulation-types/catalogs --template cmip6/cmip6.xml.j2
find circulation-types/catalogs/CMIP6/ -type f | python catalog.py --root circulation-types/catalogs/catalog.xml --name CMIP6 --template cmip6/root.xml.j2
