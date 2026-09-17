#!/usr/bin/env python3
"""Depth points as OSM from a depth raster.

    depth_points_osm.py RASTER OUTPUT.osm.gz --bbox W S E N [--shoalest SPACING] [--first-id N]

Without --shoalest every cell of RASTER (depths already averaged per cell) with a value below 0 m inside
[W, E) x [S, N) becomes a node at the cell centre: a regular grid of points.

With --shoalest, RASTER is the fine grid and the points follow chart practice: per SPACING degree cell (aligned to
multiples of SPACING, so neighbouring tiles share the cells) the shoalest depth below 0 m becomes a node at the fine
cell where it was found. The points no longer stand in rows, and a bank inside the cell is not averaged away. A cell
belongs to the box that holds its centre, so neighbouring tiles do not write it twice.

Nodes are tagged point=depth with the depth as name: one decimal down to 100 m, whole metres below. Rows are read
through GDAL, so a large raster does not need to fit in memory.
"""
import argparse
import gzip
import math
import struct

from osgeo import gdal

gdal.UseExceptions()

NONE = -1e30  # nodata for --shoalest: below any depth, so max() of a row slice never picks it


def node(node_id, lat, lon, depth):
    name = ('%.1f' % depth) if depth < 100 else ('%d' % round(depth))
    return ('<node id="-%d" visible="true" lat="%.6f" lon="%.6f"><tag k="point" v="depth"/>'
            '<tag k="name" v="%s"/></node>\n' % (node_id, lat, lon, name))


def grid_points(ds, out, w, s, e, n, node_id):
    band = ds.GetRasterBand(1)
    x0, dx, _, y0, _, dy = ds.GetGeoTransform()
    nodata = band.GetNoDataValue()
    cols, rows = ds.RasterXSize, ds.RasterYSize
    fmt = '<%df' % cols
    written = 0
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
            lines.append(node(node_id, lat, lon, -v))
            node_id += 1
        written += len(lines)
        out.write(''.join(lines))
    return written


def shoalest_points(ds, out, w, s, e, n, spacing, node_id):
    # nodata and NaN as NONE through a warped VRT, so the rows need no per-value check
    ds = gdal.Warp('', ds, format='VRT', dstNodata=NONE, outputType=gdal.GDT_Float32)
    band = ds.GetRasterBand(1)
    x0, dx, _, y0, _, dy = ds.GetGeoTransform()
    cols, rows = ds.RasterXSize, ds.RasterYSize
    fmt = '<%df' % cols

    # cells whose centre lies in the box, and the fine columns / rows (by their centres) of each
    def bounds(origin, step, count, lo, hi):
        """[(cell index, first, end)] of the fine indexes whose centres fall in each SPACING cell with centre in [lo, hi)"""
        out, current, first = [], None, 0
        for i in range(count):
            c = math.floor((origin + (i + 0.5) * step) / spacing)
            if c != current:
                if current is not None:
                    out.append((current, first, i))
                current, first = c, i
        if current is not None:
            out.append((current, first, count))
        return [b for b in out if lo <= (b[0] + 0.5) * spacing < hi]

    col_cells = bounds(x0, dx, cols, w, e)
    row_cells = bounds(y0, dy, rows, s, n)
    written = 0
    for cy, r0, r1 in row_cells:
        data = [struct.unpack(fmt, band.ReadRaster(0, r, cols, 1, buf_type=gdal.GDT_Float32)) for r in range(r0, r1)]
        lines = []
        for cx, c0, c1 in col_cells:
            best, best_r, best_c = NONE, -1, -1
            for k, values in enumerate(data):
                part = values[c0:c1]
                m = max(part)
                if m >= 0:  # drying or land height in the cell: the shoalest value below 0 m instead
                    below = [v for v in part if v < 0]
                    m = max(below) if below else NONE
                if m > best:
                    best, best_r, best_c = m, r0 + k, c0 + part.index(m)
            if best <= NONE / 2:
                continue
            lines.append(node(node_id, y0 + (best_r + 0.5) * dy, x0 + (best_c + 0.5) * dx, -best))
            node_id += 1
        written += len(lines)
        out.write(''.join(lines))
    return written


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('raster')
    parser.add_argument('output')
    parser.add_argument('--bbox', nargs=4, type=float, required=True, metavar=('W', 'S', 'E', 'N'))
    parser.add_argument('--shoalest', type=float, metavar='SPACING',
                        help='one node per SPACING degree cell at its shoalest fine cell')
    parser.add_argument('--first-id', type=int, default=1, help='ids are negative, counting down from this')
    args = parser.parse_args()
    w, s, e, n = args.bbox

    ds = gdal.Open(args.raster)
    with gzip.open(args.output, 'wt', compresslevel=5) as out:
        out.write("<?xml version='1.0' encoding='UTF-8'?>\n<osm version='0.6' upload='false' generator='depth_points_osm'>\n")
        if args.shoalest:
            written = shoalest_points(ds, out, w, s, e, n, args.shoalest, args.first_id)
        else:
            written = grid_points(ds, out, w, s, e, n, args.first_id)
        out.write('</osm>\n')
    print('depth points written: %d' % written)


if __name__ == '__main__':
    main()
