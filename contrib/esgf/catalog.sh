#!/bin/bash

set -e
set -u

usage() {
    echo 'catalog.sh [-h] [--ncmls FILE] --name NAME --namespace NAMESPACE --cut CUT --drs-ncml-prefix --drs-catalog-prefix DESTINATION' >&2
}

ncmls=""
while [[ $# -gt 0 ]]
do
    case "$1" in
    -h | --help)
        usage
        exit 1
        ;;
    --cut)
        cut="$2"
        shift 2
        ;;
    --name)
        root_name="$2"
        shift 2
        ;;
    --namespace)
        namespace="$2"
        shift 2
        ;;
    --ncmls)
        ncmls="$2"
        shift 2
        ;;
    --drs-ncml-prefix)
        drs_ncml_prefix="$2"
        shift 2
        ;;
    --drs-catalog-prefix)
        drs_catalog_prefix="$2"
        shift 2
        ;;
    -*)
        usage
        exit 1
        ;;
    *)
        catalogs="$1"
        break
        ;;
    esac
done

if [ -z "$cut" ]; then
    # This is valid for cmip6 and cordex
    cut='1-5,7-'
fi

ref() {
    echo '  <catalogRef xlink:title="'"$title"'" xlink:href="'$href'" name="">'
    echo '    <dataSize units="bytes">'"$size"'</dataSize>'
    echo '    <date type="modified">'"$last_modified"'</date>'
    echo '  </catalogRef>' 
    echo ''
}

dataset() {
    echo '  <dataset name="'$name'"'
    echo '      ID="'$namespace'/'$drs'/'$name'"'
    echo '      urlPath="'$namespace'/'$drs'/'$name'">'
    echo '    <metadata inherited="true">'
    echo '      <serviceName>virtual</serviceName>'
    echo '      <dataSize units="bytes">'"$size"'</dataSize>'
    echo '      <date type="modified">'"$last_modified"'</date>'
    echo '    </metadata>'
    echo '    <netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2"'
    echo '            location="content/'$public'" />'
    echo '  </dataset>'
    echo ''
}

init_catalog() {
    cat <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<catalog name="$drs"
        xmlns="http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0"
        xmlns:xlink="http://www.w3.org/1999/xlink">
  <service name="virtual" serviceType="Compound" base="">
    <service base="/thredds/dodsC/" name="odap" serviceType="OpenDAP"/>
    <service base="/thredds/dap4/" name="dap4" serviceType="DAP4" />
    <service base="/thredds/wcs/" name="wcs" serviceType="WCS" />
    <service base="/thredds/wms/" name="wms" serviceType="WMS" />
    <service base="/thredds/ncss/grid/" name="ncssGrid" serviceType="NetcdfSubset" />
    <service base="/thredds/ncss/point/" name="ncssPoint" serviceType="NetcdfSubset" />
    <service base="/thredds/cdmremote/" name="cdmremote" serviceType="CdmRemote" />
    <service base="/thredds/cdmrfeature/grid/" name="cdmrFeature" serviceType="CdmrFeature" />
    <service base="/thredds/iso/" name="iso" serviceType="ISO" />
    <service base="/thredds/ncml/" name="ncml" serviceType="NCML" />
    <service base="/thredds/uddc/" name="uddc" serviceType="UDDC" />
  </service>
EOF
}

# Insert datasets into catalogs
sort -V "${ncmls:--}" | while read ncml
do
    basename=${ncml##*/}
    name=${basename%.ncml}
    last_modified=$(stat --format='%z' "$ncml")
    size=$(sed -n '/attribute name="size"/{s/[^0-9]//g;p}' $ncml)
    
    drs=$(echo $name | cut -d_ -f"$cut")
    drs=${drs//_/\/}
    
    public=${ncml#${drs_ncml_prefix}}
    catalog="${catalogs}/${drs}/catalog.xml"

    # Init catalog if it does not exist
    if [ ! -f "$catalog" ]; then
        mkdir -p ${catalogs}/${drs}
        init_catalog >${catalogs}/${drs}/catalog.xml
    fi

    dataset $ncml >> $catalog
done

# Close catalogs
find $catalogs -type f | while read catalog
do
    echo '</catalog>' >> $catalog
    echo $catalog
done

# Generate root catalog
root="${catalogs}/catalog.xml"
cat > ${root} <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<catalog name="$root_name"
        xmlns="http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0"
        xmlns:xlink="http://www.w3.org/1999/xlink">
EOF

find $catalogs -mindepth 3 -type f | sort -V | while read catalog
do
    title=${catalog%/catalog.xml}
    title=${title#${drs_catalog_prefix}}
    title=${title//\//_}
    size=$(sed -n "/dataSize/{s/[^0-9]//g;p}" $catalog | awk '{sum+=$0}END{print sum}')
    last_modified=$(stat --format='%z' $catalog)
    
    href="${title//_//}/catalog.xml"
    ref >> $root
done

echo '</catalog>' >> $root
echo $root
