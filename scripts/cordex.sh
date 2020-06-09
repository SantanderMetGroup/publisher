#!/bin/bash

set -e

ncs="/oceano/gmeteo/WORK/PROYECTOS/2020_C3S_34d/synda/data/cordex"
ncmls="/oceano/gmeteo/WORK/PROYECTOS/2020_C3S_34d/tds/content/thredds/public/cordex/output"
content="tds-content"

# Just for testing purposes
#find $ncs -type f | sed 200q | python ncml.py --adapter CordexNcmlAdapter --dest tds-content/public
#find tests/cordex/tds-content/public/CORDEX -type f | python catalog.py --adapter CordexCatalogAdapter --dest tests/cordex/tds-content
#find tests/cordex/tds-content/CORDEX/output -type f | python catalog.py --adapter CordexCatalogAdapter --root --dest tests/cordex/tds-content/CORDEX/catalog.xml
find ${ncmls} -name '*.ncml' -type f | grep -v -F 'EUR-' | python catalog.py --adapter CordexCatalogAdapter --dest ${content}/devel/c3s34d
find ${content}/devel/c3s34d/CORDEX -type f | python catalog.py --adapter CordexCatalogAdapter --root --dest ${content}/devel/c3s34d/catalog.xml
