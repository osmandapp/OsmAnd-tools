#!/usr/bin/env python3
"""Depth points as OSM from a depth raster.

    depth_points_osm.py RASTER OUTPUT.osm.gz --bbox W S E N [--shoalest SPACING] [--thin] [--first-id N]

Without --shoalest every cell of RASTER (depths already averaged per cell) with a value below 0 m inside
[W, E) x [S, N) becomes a node at the cell centre: a regular grid of points.

With --shoalest, RASTER is the fine grid and the points follow chart practice: per SPACING degree cell (aligned to
multiples of SPACING, so neighbouring tiles share the cells) the shoalest depth below 0 m becomes a node at the fine
cell where it was found. The points no longer stand in rows, and a bank inside the cell is not averaged away. A cell
belongs to the box that holds its centre, so neighbouring tiles do not write it twice.

--thin keeps a sounding only where it says something: a cell shoaler than all its neighbours (a bank), or one whose
depth differs from the average of its neighbours by more than 0.5 m and 3 per cent (a slope break, a channel edge).
On an even bottom, where every cell repeats its neighbours, the shoalest cell of every three by three block is kept,
so the chart does not go empty and its soundings still follow the bottom instead of standing in rows.

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


def thin_out(cells):
    """Cell indexes to leave out: their depth is what the neighbours already say. The shoalest cell of a
    neighbourhood (a bank) and a cell that breaks the slope are kept, and so is every third cell of an even bottom."""
    drop = set()
    for (cx, cy), (v, _, _) in cells.items():
        around = [cells[(cx + i, cy + j)][0] for i in (-1, 0, 1) for j in (-1, 0, 1)
                  if (i or j) and (cx + i, cy + j) in cells]
        if len(around) < 8:  # at the edge of the data, where the neighbours are unknown
            continue
        if v >= max(around):  # shoaler than all of them
            continue
        if abs(v - sum(around) / len(around)) > max(0.5, 0.03 * abs(v)):
            continue
        drop.add((cx, cy))
    # one sounding of every three by three cells stays even on an even bottom: the shoalest of the block, so that its
    # place follows the bottom instead of standing in a row
    blocks = {}
    for cx, cy in drop:
        key = (cx // 3, cy // 3)
        if key not in blocks or cells[(cx, cy)][0] > cells[blocks[key]][0]:
            blocks[key] = (cx, cy)
    return drop - set(blocks.values())


def shoalest_points(ds, out, w, s, e, n, spacing, node_id, thin=False):
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
    cells = {}
    for cy, r0, r1 in row_cells:
        data = [struct.unpack(fmt, band.ReadRaster(0, r, cols, 1, buf_type=gdal.GDT_Float32)) for r in range(r0, r1)]
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
            if best > NONE / 2:
                cells[(cx, cy)] = (best, y0 + (best_r + 0.5) * dy, x0 + (best_c + 0.5) * dx)
    drop = thin_out(cells) if thin else set()
    written = 0
    for key in sorted(cells, key=lambda k: (-k[1], k[0])):
        if key in drop:
            continue
        v, lat, lon = cells[key]
        out.write(node(node_id, lat, lon, -v))
        node_id += 1
        written += 1
    return written


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('raster')
    parser.add_argument('output')
    parser.add_argument('--bbox', nargs=4, type=float, required=True, metavar=('W', 'S', 'E', 'N'))
    parser.add_argument('--shoalest', type=float, metavar='SPACING',
                        help='one node per SPACING degree cell at its shoalest fine cell')
    parser.add_argument('--thin', action='store_true', help='with --shoalest: leave out the soundings a neighbour already tells')
    parser.add_argument('--first-id', type=int, default=1, help='ids are negative, counting down from this')
    args = parser.parse_args()
    w, s, e, n = args.bbox

    ds = gdal.Open(args.raster)
    with gzip.open(args.output, 'wt', compresslevel=5) as out:
        out.write("<?xml version='1.0' encoding='UTF-8'?>\n<osm version='0.6' upload='false' generator='depth_points_osm'>\n")
        if args.shoalest:
            written = shoalest_points(ds, out, w, s, e, n, args.shoalest, args.first_id, args.thin)
        else:
            written = grid_points(ds, out, w, s, e, n, args.first_id)
        out.write('</osm>\n')
    print('depth points written: %d' % written)


if __name__ == '__main__':
    main()
