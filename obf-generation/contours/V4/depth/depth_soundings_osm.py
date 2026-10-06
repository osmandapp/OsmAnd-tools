#!/usr/bin/env python3
"""Depth points as OSM from measured soundings (a point layer with a depth field), thinned per zoom tier.

    depth_soundings_osm.py SOURCE OUTPUT_PREFIX --tiers "0.02 0.005 0.001" [--layer dybdepunkt] [--field dybde]
                           [--bbox W S E N] [--land LAND] [--land-cell DEG] [--first-id N]

For every tier the soundings are gridded by SPACING degrees and the shallowest one of each cell is kept - the safe
one, as on a paper chart. Tier i goes to OUTPUT_PREFIXi.osm.gz as nodes tagged point=depth with the depth as name
(one decimal down to 100 m, whole metres below), the same tags as depth_points_osm.py. Soundings of 0 m and above
(drying heights) and soundings on OSM land (LAND polygons, when given) are left out. With --land-cell the land is
rasterized in 1 degree blocks and looked up per sounding: a point-in-polygon test against a whole continent polygon
takes 0.16 s. With a bbox only the soundings with W <= lon < E and S <= lat < N are read, so tiles do not repeat any.
"""
import argparse
import gzip
import math

from osgeo import gdal, ogr

ogr.UseExceptions()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('source')
    parser.add_argument('output_prefix')
    parser.add_argument('--tiers', required=True, help='cell sizes in degrees, one per tier')
    parser.add_argument('--layer', default='dybdepunkt')
    parser.add_argument('--field', default='dybde')
    parser.add_argument('--bbox', nargs=4, type=float, metavar=('W', 'S', 'E', 'N'))
    parser.add_argument('--land', help='land polygons (OGR source); soundings inside them are dropped')
    parser.add_argument('--land-cell', type=float, help='look soundings up in LAND rasterized at this cell (degrees)')
    parser.add_argument('--first-id', type=int, default=1, help='ids are negative, counting down from this')
    args = parser.parse_args()
    spacings = [float(s) for s in args.tiers.split()]

    src = ogr.Open(args.source)  # the layer is only valid while its data source is referenced
    soundings = src.GetLayer(args.layer)
    if args.bbox:
        soundings.SetSpatialFilterRect(*args.bbox)
    # tier -> cell -> (depth, lon, lat)
    cells = [{} for _ in spacings]
    read = 0
    for f in soundings:
        depth = f.GetField(args.field)
        g = f.GetGeometryRef()
        if depth is None or depth <= 0 or g is None:
            continue
        lon, lat = g.GetX(), g.GetY()
        if args.bbox and not (args.bbox[0] <= lon < args.bbox[2] and args.bbox[1] <= lat < args.bbox[3]):
            continue
        read += 1
        for t, sp in enumerate(spacings):
            key = (math.floor(lon / sp), math.floor(lat / sp))
            best = cells[t].get(key)
            if best is None or depth < best[0]:
                cells[t][key] = (depth, lon, lat)

    land_src = ogr.Open(args.land) if args.land else None
    land = land_src.GetLayer(0) if land_src else None
    on_land = {}
    blocks = {}

    def land_block(i, j):
        """cells of a 1 degree block as bytes, 1 on land: rasterizing the land once is much cheaper than a
        point-in-polygon test per sounding"""
        if (i, j) not in blocks:
            c = args.land_cell
            size = round(1 / c)
            mask = gdal.GetDriverByName('MEM').Create('', size, size, 1, gdal.GDT_Byte)
            mask.SetGeoTransform((i, c, 0, j + 1, 0, -c))
            # clipped to the block first: burning a whole continent polygon takes a minute per block
            block = ogr.CreateGeometryFromWkt('POLYGON((%d %d,%d %d,%d %d,%d %d,%d %d))' % (
                i, j, i + 1, j, i + 1, j + 1, i, j + 1, i, j))
            # vector 'Memory' became part of 'MEM' in GDAL 3.11
            clipped = ogr.GetDriverByName('MEM' if int(gdal.VersionInfo()) >= 3110000 else 'Memory').CreateDataSource('')
            layer = clipped.CreateLayer('land', geom_type=ogr.wkbUnknown)
            land.SetSpatialFilterRect(i, j, i + 1, j + 1)
            for f in land:
                part = ogr.Feature(layer.GetLayerDefn())
                part.SetGeometry(f.GetGeometryRef().Intersection(block))
                layer.CreateFeature(part)
            gdal.RasterizeLayer(mask, [1], layer, burn_values=[1])
            blocks[(i, j)] = mask.GetRasterBand(1).ReadRaster()
        return blocks[(i, j)]

    def is_land(lon, lat):
        if land is None:
            return False
        if args.land_cell:
            i, j = math.floor(lon), math.floor(lat)
            size = round(1 / args.land_cell)
            x, y = min(int((lon - i) * size), size - 1), min(int((j + 1 - lat) * size), size - 1)
            return land_block(i, j)[y * size + x] == 1
        if (lon, lat) not in on_land:
            p = ogr.Geometry(ogr.wkbPoint)
            p.AddPoint_2D(lon, lat)
            land.SetSpatialFilter(p)
            on_land[(lon, lat)] = any(poly.GetGeometryRef().Contains(p) for poly in land)
        return on_land[(lon, lat)]

    node_id = args.first_id
    for t, sp in enumerate(spacings):
        written = 0
        with gzip.open('%s%d.osm.gz' % (args.output_prefix, t + 1), 'wt', compresslevel=5) as out:
            out.write("<?xml version='1.0' encoding='UTF-8'?>\n<osm version='0.6' upload='false' generator='depth_soundings_osm'>\n")
            for depth, lon, lat in cells[t].values():
                if is_land(lon, lat):
                    continue
                name = ('%.1f' % depth) if depth < 100 else ('%d' % round(depth))
                out.write('<node id="-%d" visible="true" lat="%.7f" lon="%.7f"><tag k="point" v="depth"/>'
                          '<tag k="name" v="%s"/></node>\n' % (node_id, lat, lon, name))
                node_id += 1
                written += 1
            out.write('</osm>\n')
        print('tier %d, %g deg: %d of %d soundings' % (t + 1, sp, written, read))


if __name__ == '__main__':
    main()
