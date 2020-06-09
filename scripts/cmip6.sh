#!/bin/bash

set -e

ncs=/oceano/gmeteo/DATA/ESGF/REPLICA/DATA/CMIP6
#logs=logs
content=tds-content
ncmls=${content}/public
catalogs=${content}/devel/atlas

# Just for testing purposes
#ncmls=tmp/cmip6/content/thredds/public
#catalogs=tmp/cmip6/content/thredds
find $ncs -type f | sed 30q | python ncml.py --adapter Cmip6NcmlAdapter --dest $ncmls

#find $ncs -mindepth 3 -maxdepth 3 -type d | parallel "find {} -type f -name '*.nc' | python ncml.py --adapter Cmip6NcmlAdapter --dest $ncmls >$logs/ncml.out.{/.} 2>$logs/ncml.err.{/.}"

find "${ncmls}" -type f | python catalog.py --adapter Cmip6CatalogAdapter --dest "${catalogs}"
find ${catalogs}/cmip6/* -mindepth 2 -type f | python catalog.py --adapter Cmip6CatalogAdapter --root --dest "${catalogs}/cmip6/catalog.xml"
