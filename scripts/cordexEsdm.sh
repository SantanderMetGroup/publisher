#!/bin/bash

set -e

# Updating these values requires update cordexEsdm.xml.j2 nc_prefix variable
ecearth_r12i1p1="/oceano/gmeteo/WORK/bmedina/CORDEX-ESDM/EC-EARTH_r12i1p1"
interim="/oceano/gmeteo/WORK/bmedina/CORDEX-ESDM/ERA-Interim"

# Updating this value requires update cordexEsdm.xml.j2 ncml_prefix variable
ncmls="/home/zequi/tds-content/public/cordex-esdm"
#ncmls="jorge-ncmls"

catalogs="/home/zequi/tds-content/cordex-esdm"
#catalogs="jorge-catalogs"
catalog_name="cordex-esdm"

# ecearth ncml
find "$ecearth_r12i1p1" -type f -not -path '*/.*' | \
  python ncml.py --adapter EcearthCordexEsdmNcmlAdapter --dest "$ncmls"

# interim ncml
find "$interim" -type f -not -path '*/.*' | \
  python ncml.py --adapter InterimCordexEsdmNcmlAdapter --dest "$ncmls"

# ecearth catalog
find "$ncmls" "$ecearth_r12i1p1" -not -path '*/.*' -type f | grep EC-EARTH | \
  python catalog.py --adapter EcearthCordexEsdmCatalogAdapter --dest "$catalogs" --template cordexEsdm/cordexEsdm.xml.j2 --name "$catalog_name"

# interim catalog
find "$ncmls" "$interim" -not -path '*/.*' -type f | grep Interim | \
  python catalog.py --adapter InterimCordexEsdmCatalogAdapter --dest "$catalogs" --template cordexEsdm/cordexEsdm.xml.j2 --name "$catalog_name"

# root catalog
find "$catalogs"/{EC-EARTH,ERA-Interim-ESD} -type f | \
  python catalog.py --root "$catalogs"/catalog.xml --name "$catalog_name" --template cordexEsdm/root.xml.j2
