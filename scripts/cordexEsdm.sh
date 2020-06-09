#!/bin/bash

set -e

ecearth_r12i1p1="/oceano/gmeteo/WORK/bmedina/CORDEX-ESDM/EC-EARTH_r12i1p1"
interim="/oceano/gmeteo/WORK/bmedina/CORDEX-ESDM/ERA-Interim"

ncmls="/home/zequi/tds-content/public/cordex-esdm"

catalogs="/home/zequi/tds-content/cordex-esdm"
catalog_name="cordex-esdm"

# Testing values
ncmls=~/tmp/cordexEsdm/content/thredds/public/cordex-esdm
catalogs=~/tmp/cordexEsdm/content/thredds/cordex-esdm

# ecearth ncml
find "$ecearth_r12i1p1" -type f -not -path '*/.*' | \
  python ncml.py --adapter EcearthCordexEsdmNcmlAdapter --dest "$ncmls"

# interim ncml
find "$interim" -type f -not -path '*/.*' | \
  python ncml.py --adapter InterimCordexEsdmNcmlAdapter --dest "$ncmls"

# ecearth catalog
find "$ncmls" "$ecearth_r12i1p1" -not -path '*/.*' -type f | grep EC-EARTH | python catalog.py --adapter EcearthCordexEsdmCatalogAdapter --dest "$catalogs"

# interim catalog
find "$ncmls" "$interim" -not -path '*/.*' -type f | grep Interim | python catalog.py --adapter InterimCordexEsdmCatalogAdapter --dest "$catalogs"

# root catalog
find "$catalogs"/{EC-EARTH,ERA-Interim-ESD} -type f | python catalog.py --adapter CordexEsdmCatalogAdapter --root --dest "${catalogs}/catalog.xml"
