#!/usr/bin/env python3
# Smooth a merged (tile + neighbours) DEM before gdal_contour.
#
# Neighbouring 1x1 degree tiles are processed separately, so the smoothed values in their overlap
# must be identical, otherwise contours do not meet at the tile seam:
#  - every resampling grid is aligned to the global pixel grid (-tr/-tap), never derived from the
#    raster size (-ts), which differs per tile;
#  - the high latitude smoothing (|lat| >= 65: downscale x2 and upscale back) is chosen per pixel row
#    by its latitude, not per tile, so both tiles at the 65 degree seam see the same field.
# The input must be on a grid aligned to its own pixel size (gdalwarp -tr ... -tap), as
# make-contour-tile-mt produces it.

import json
import os
import subprocess
import sys

HIGH_LAT = 65.0
WARP_OPTIONS = ['-r', 'cubicspline', '-ot', 'Float32', '-co', 'COMPRESS=LZW', '-wo', 'NUM_THREADS=4', '-multi']


def run(cmd):
    subprocess.run(cmd, check=True)


def raster_info(path):
    info = json.loads(subprocess.check_output(['gdalinfo', '-json', path]))
    gt = info['geoTransform']
    width, height = info['size']
    return gt[0], gt[3], gt[1], -gt[5], width, height


def smooth_low(src, dst):
    run(['gdalwarp', '-overwrite'] + WARP_OPTIONS + [src, dst])


def smooth_high(src, dst):
    left, top, xres, yres, width, height = raster_info(src)
    coarse = dst + '.coarse.tif'
    run(['gdalwarp', '-overwrite', '-tr', repr(2 * xres), repr(2 * yres), '-tap'] + WARP_OPTIONS + [src, coarse])
    run(['gdalwarp', '-overwrite', '-tr', repr(xres), repr(yres),
         '-te', repr(left), repr(top - height * yres), repr(left + width * xres), repr(top)]
        + WARP_OPTIONS + [coarse, dst])
    os.remove(coarse)


def is_high(row_lat):
    return abs(row_lat) >= HIGH_LAT


def smooth_dem(src, dst):
    _, top, _, yres, width, height = raster_info(src)
    high_rows = [is_high(top - (r + 0.5) * yres) for r in range(height)]
    if not any(high_rows):
        smooth_low(src, dst)
        return
    if all(high_rows):
        smooth_high(src, dst)
        return
    low_path = dst + '.low.tif'
    high_path = dst + '.high.tif'
    smooth_low(src, low_path)
    smooth_high(src, high_path)
    parts = []
    start = 0
    for r in range(1, height + 1):
        if r == height or high_rows[r] != high_rows[start]:
            part = '%s.part%d.tif' % (dst, len(parts))
            run(['gdal_translate', '-q', '-srcwin', '0', str(start), str(width), str(r - start),
                 high_path if high_rows[start] else low_path, part])
            parts.append(part)
            start = r
    vrt = dst + '.vrt'
    run(['gdalbuildvrt', '-q', vrt] + parts)
    run(['gdal_translate', '-q', '-co', 'COMPRESS=LZW', vrt, dst])
    for f in parts + [vrt, low_path, high_path]:
        os.remove(f)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: smooth_dem.py input.tif output.tif')
        sys.exit(1)
    smooth_dem(sys.argv[1], sys.argv[2])
