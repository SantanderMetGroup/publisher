#!/bin/bash

set -e

ncs="/oceano/gmeteo/DATA/ESGF/REPLICA/DATA/CMIP6"
logs=~/publisher/tests/cmip6
ncmls=~/publisher/tests/cmip6/content/thredds/public
catalogs=~/publisher/tests/cmip6/content/thredds

# Just for testing purposes
ncmls=tmp/cmip6/content/thredds/public
catalogs=tmp/cmip6/content/thredds
find $ncs -type f | sed 300q | python ncml.py --adapter Cmip6NcmlAdapter --dest $ncmls

#find $ncs -mindepth 3 -maxdepth 3 -type d | parallel "find {} -type f | python ncml.py --adapter Cmip6NcmlAdapter --dest $ncmls >$logs/ncml.out.{/.} 2>$logs/ncml.err.{/.}"

find "${ncmls}" -type f | python catalog.py --adapter Cmip6CatalogAdapter --dest "${catalogs}"
find ${catalogs}/cmip6/* -mindepth 2 -type f | python catalog.py --adapter Cmip6CatalogAdapter --root "${catalogs}/cmip6/catalog.xml" --name CMIP6 --template cmip6/root.xml.j2
