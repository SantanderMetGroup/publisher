#!/bin/bash

# $1 title
# $2 href
# $3 size
# $4 last_modified
ref1() {
    echo '  <catalogRef xlink:title="'"$1"'" xlink:href="'$2'" name="">'
    echo '    <dataSize units="bytes">'"$3"'</dataSize>'
    echo '    <date type="modified">'"$4"'</date>'
    echo '  </catalogRef>' 
    echo ''
}

# $1 name
# $2 id
# $3 urlPath
# $4 size
# $5 last_modified
# $6 location
dataset1() {
    echo '  <dataset name="'$1'"'
    echo '      ID="'$2'"'
    echo '      urlPath="'$3'">'
    echo '    <metadata inherited="true">'
    echo '      <serviceName>virtual</serviceName>'
    echo '      <dataSize units="bytes">'"$4"'</dataSize>'
    echo '      <date type="modified">'"$5"'</date>'
    echo '    </metadata>'
    echo '    <netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2"'
    echo '            location="'$6'" />'
    echo '  </dataset>'
    echo ''
}

init_catalog() {
    echo '<?xml version="1.0" encoding="UTF-8"?>'
    echo "<catalog name=\"$1\""
    echo '         xmlns="http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0"'
    echo '         xmlns:xlink="http://www.w3.org/1999/xlink">'
    echo ''
    echo '  <service name="virtual" serviceType="Compound" base="">'
    echo '    <service base="/thredds/dodsC/" name="odap" serviceType="OpenDAP"/>'
    echo '    <service base="/thredds/dap4/" name="dap4" serviceType="DAP4" />'
    echo '    <service base="/thredds/wcs/" name="wcs" serviceType="WCS" />'
    echo '    <service base="/thredds/wms/" name="wms" serviceType="WMS" />'
    echo '    <service base="/thredds/ncss/grid/" name="ncssGrid" serviceType="NetcdfSubset" />'
    echo '    <service base="/thredds/ncss/point/" name="ncssPoint" serviceType="NetcdfSubset" />'
    echo '    <service base="/thredds/cdmremote/" name="cdmremote" serviceType="CdmRemote" />'
    echo '    <service base="/thredds/cdmrfeature/grid/" name="cdmrFeature" serviceType="CdmrFeature" />'
    echo '    <service base="/thredds/iso/" name="iso" serviceType="ISO" />'
    echo '    <service base="/thredds/ncml/" name="ncml" serviceType="NCML" />'
    echo '    <service base="/thredds/uddc/" name="uddc" serviceType="UDDC" />'
    echo '  </service>'
    echo ''
}
