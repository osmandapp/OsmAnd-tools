#!/usr/bin/env python3
"""Depth points as OSM from a raster of averaged depths.

    depth_points_osm.py RASTER OUTPUT.osm.gz --bbox W S E N [--first-id N]

Every cell with a value below 0 m inside [W, E) x [S, N) becomes a node tagged point=depth with the depth as name:
one decimal down to 100 m, whole metres below. Cells are read row by row through GDAL, so a large raster does not
need to fit in memory; the box keeps neighbouring tiles from writing the same cell twice.
"""
import argparse
import gzip
import math
import struct

from osgeo import gdal

gdal.UseExceptions()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('raster')
    parser.add_argument('output')
    parser.add_argument('--bbox', nargs=4, type=float, required=True, metavar=('W', 'S', 'E', 'N'))
    parser.add_argument('--first-id', type=int, default=1, help='ids are negative, counting down from this')
    args = parser.parse_args()
    w, s, e, n = args.bbox

    ds = gdal.Open(args.raster)
    band = ds.GetRasterBand(1)
    x0, dx, _, y0, _, dy = ds.GetGeoTransform()
    nodata = band.GetNoDataValue()
    cols, rows = ds.RasterXSize, ds.RasterYSize
    fmt = '<%df' % cols
    node_id = args.first_id
    written = 0
    with gzip.open(args.output, 'wt', compresslevel=5) as out:
        out.write("<?xml version='1.0' encoding='UTF-8'?>\n<osm version='0.6' upload='false' generator='depth_points_osm'>\n")
        for row in range(rows):
            lat = y0 + (row + 0.5) * dy
            if not (s <= lat < n):
                continue
            values = struct.unpack(fmt, band.ReadRaster(0, row, cols, 1, buf_type=gdal.GDT_Float32))
            lines = []
            for col, v in enumerate(values):
                if math.isnan(v) or v >= 0 or (nodata is not None and v == nodata):
                    continue
                lon = x0 + (col + 0.5) * dx
                if not (w <= lon < e):
                    continue
                depth = -v
                name = ('%.1f' % depth) if depth < 100 else ('%d' % round(depth))
                lines.append('<node id="-%d" visible="true" lat="%.6f" lon="%.6f"><tag k="point" v="depth"/>'
                             '<tag k="name" v="%s"/></node>\n' % (node_id, lat, lon, name))
                node_id += 1
            written += len(lines)
            out.write(''.join(lines))
        out.write('</osm>\n')
    print('depth points written: %d' % written)


if __name__ == '__main__':
    main()
