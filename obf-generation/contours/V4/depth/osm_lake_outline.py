#!/usr/bin/env python3
"""Water outlines of the lakes as GeoPackage polygons from OpenStreetMap (Nominatim).

    osm_lake_outline.py OUTPUT.gpkg --lake ID="SEARCH NAME" [--lake ...] [--threshold DEG] [--refresh]

The raster grid knows the shore only as the edge of a two-metre cell, so a depth band traced from it
is a staircase and reaches onto the land.  The outline stored here is the water polygon the map
itself draws, and depth_areas_osm.py --water cuts the bands with it.

Nominatim asks for one request per second and a descriptive agent; the answer is cached in the
GeoPackage, so a rebuild does not hit the server again unless --refresh is given.
"""
import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request

from osgeo import ogr, osr

ogr.UseExceptions()

UA = 'OsmAnd-depth-maps/1.0 (lake depth areas build)'


def fetch(name, threshold):
    query = urllib.parse.urlencode({
        'q': name, 'format': 'json', 'polygon_geojson': 1,
        'polygon_threshold': threshold, 'limit': 1,
    })
    url = 'https://nominatim.openstreetmap.org/search?' + query
    req = urllib.request.Request(url, headers={'User-Agent': UA})
    with urllib.request.urlopen(req, timeout=120) as r:
        data = json.load(r)
    if not data:
        return None
    geo = data[0].get('geojson')
    if not geo:
        return None
    return geo


def to_ogr(geo):
    """GeoJSON geometry (Polygon or MultiPolygon) to an OGR polygon."""
    def ring(coords):
        r = ogr.Geometry(ogr.wkbLinearRing)
        for lon, lat in coords:
            r.AddPoint_2D(lon, lat)
        return r

    t = geo.get('type')
    if t == 'Polygon':
        parts = [geo['coordinates']]
    elif t == 'MultiPolygon':
        parts = geo['coordinates']
    else:
        return None
    mp = ogr.Geometry(ogr.wkbMultiPolygon)
    for part in parts:
        if not part or len(part[0]) < 4:
            continue
        p = ogr.Geometry(ogr.wkbPolygon)
        p.AddGeometry(ring(part[0]))
        for hole in part[1:]:
            if len(hole) >= 4:
                p.AddGeometry(ring(hole))
        mp.AddGeometry(p)
    return mp if mp.GetGeometryCount() else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('output')
    parser.add_argument('--lake', action='append', required=True, metavar='ID=NAME',
                        help='lake id of the build and the name to look up, e.g. thunersee=Thunersee')
    parser.add_argument('--threshold', type=float, default=0.00002,
                        help='polygon simplification of the answer in degrees, default 0.00002 (about 2 m)')
    parser.add_argument('--refresh', action='store_true', help='ask again even when the lake is cached')
    args = parser.parse_args()

    lakes = []
    for spec in args.lake:
        lid, _, name = spec.partition('=')
        lakes.append((lid, name or lid))

    have = {}
    if os.path.exists(args.output) and not args.refresh:
        src = ogr.Open(args.output)
        if src is not None:
            for f in src.GetLayer(0):
                have[f.GetField('lake')] = f.GetGeometryRef().Clone()
            src = None
            print('%d lake outlines cached in %s' % (len(have), args.output))

    drv = ogr.GetDriverByName('GPKG')
    if os.path.exists(args.output):
        drv.DeleteDataSource(args.output)
    out = drv.CreateDataSource(args.output)
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    layer = out.CreateLayer('water', srs, ogr.wkbMultiPolygon)
    layer.CreateField(ogr.FieldDefn('lake', ogr.OFTString))

    def add_feature(lid, geom):
        if not geom.IsValid():
            geom = geom.MakeValid()
        f = ogr.Feature(layer.GetLayerDefn())
        f.SetField('lake', lid)
        f.SetGeometry(geom)
        layer.CreateFeature(f)

    missing = 0
    for lid, name in lakes:
        geom = have.get(lid)
        if geom is None:
            try:
                geo = fetch(name, args.threshold)
            except Exception as e:
                print('%-18s %s' % (lid, e), file=sys.stderr)
                geo = None
            geom = to_ogr(geo) if geo else None
            if geom is None:
                print('%-18s no outline for "%s"' % (lid, name), file=sys.stderr)
                missing += 1
                continue
            time.sleep(1.1)   # Nominatim: at most one request per second
        add_feature(lid, geom)
        print('%-18s "%-20s" area %.4f deg2, %d parts'
              % (lid, name, geom.GetArea(), geom.GetGeometryCount()))
    # A build of a subset must not drop the outlines other builds share the file with.
    requested = {lid for lid, _ in lakes}
    kept = 0
    for lid, geom in have.items():
        if lid not in requested:
            add_feature(lid, geom)
            kept += 1
    if kept:
        print('%d other cached outlines kept' % kept)
    out = None
    if missing:
        sys.exit('%d lake outline(s) not found' % missing)


if __name__ == '__main__':
    main()
