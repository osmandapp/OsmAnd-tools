#!/usr/bin/env python3
"""Depth points as OSM from measured soundings (a point layer with a depth field), thinned per zoom tier.

    depth_soundings_osm.py SOURCE OUTPUT_PREFIX --tiers "0.02 0.005 0.001" [--layer dybdepunkt] [--field dybde]
                           [--bbox W S E N] [--land LAND] [--first-id N]

For every tier the soundings are gridded by SPACING degrees and the shallowest one of each cell is kept - the safe
one, as on a paper chart. Tier i goes to OUTPUT_PREFIXi.osm.gz as nodes tagged point=depth with the depth as name
(one decimal down to 100 m, whole metres below), the same tags as depth_points_osm.py. Soundings of 0 m and above
(drying heights) and soundings on OSM land (LAND polygons, when given) are left out.
"""
import argparse
import gzip
import math

from osgeo import ogr

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
    parser.add_argument('--first-id', type=int, default=1, help='ids are negative, counting down from this')
    args = parser.parse_args()
    spacings = [float(s) for s in args.tiers.split()]

    src = ogr.Open(args.source)  # the layer is only valid while its data source is referenced
    layer = src.GetLayer(args.layer)
    if args.bbox:
        layer.SetSpatialFilterRect(*args.bbox)
    # tier -> cell -> (depth, lon, lat)
    cells = [{} for _ in spacings]
    read = 0
    for f in layer:
        depth = f.GetField(args.field)
        g = f.GetGeometryRef()
        if depth is None or depth <= 0 or g is None:
            continue
        read += 1
        lon, lat = g.GetX(), g.GetY()
        for t, sp in enumerate(spacings):
            key = (math.floor(lon / sp), math.floor(lat / sp))
            best = cells[t].get(key)
            if best is None or depth < best[0]:
                cells[t][key] = (depth, lon, lat)

    land_src = ogr.Open(args.land) if args.land else None
    land = land_src.GetLayer(0) if land_src else None
    on_land = {}

    def is_land(lon, lat):
        if land is None:
            return False
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
