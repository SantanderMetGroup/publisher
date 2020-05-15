#!/bin/bash

set -e

ncs="/oceano/gmeteo/WORK/PROYECTOS/2020_C3S_34d/synda/data/cordex"

# Just for testing purposes
find $ncs -type f | sed 200q | python ncml.py --adapter CordexNcmlAdapter --dest tests/cordex/tds-content/public
find tests/cordex/tds-content/public/CORDEX -type f | python catalog.py --adapter CordexCatalogAdapter --dest tests/cordex/tds-content
find tests/cordex/tds-content/CORDEX/output -type f | python catalog.py --root tests/cordex/tds-content/CORDEX/catalog.xml --name CORDEX --template cordex/root.xml.j2
