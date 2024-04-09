#!/bin/bash

cd /dataCOPY/users/Public/emile.sonneveld/ERA5-Land-monthly-averaged-data-ANIN/ || exit

infile=ERA5_monthly.nc
# Hard coded band names:
for band_name in tp d2m sp ssrd u10 v10; do
  band=1
  # sudo apt-get install -y cdo
  for idate in $(cdo showdate $infile); do
    echo "band: $band"
    date="${idate:0:10}"
    echo "date: $date"
    # filter out non-date entries:
    if [[ ${date} =~ [0-9]{4}-[0-9]{2}-[0-9]{2} ]]; then
      y="${idate:0:4}"
      m="${idate:5:2}"
      d="${idate:8:2}"
      mkdir -p tiff_collection_unscale/$y/$m/$d
      gdal_translate -co COMPRESS=DEFLATE -unscale -mo add_offset=0 -mo scale_factor=1 -ot Float32 NETCDF:$infile://$band_name -b $band "tiff_collection_unscale/$y/$m/$d/${date}_$band_name.tif"
      ((band++))
    fi
  done
done
echo "All done"
