#!/usr/bin/env python3
"""Charted depth contours and soundings from S-57 ENC cells (NOAA ENC_ROOT), the most detailed cell winning.

    depth_enc_osm.py CELL_OR_DIR... --bbox W S E N --contours OUT.osm.gz --minor OUT.osm.gz --soundings OUT.gpkg
                     --areas OUT.gpkg --coverage OUT.gpkg [--bands 4,5,6]

Cells are the *.000 files (updates *.001... next to them are applied by GDAL) in the given folders, or given as files;
only the navigational purposes (usage band, third character of the name) of --bands are read. Where a more detailed
band covers an area (M_COVR, CATCOV=1), the contours and soundings of the less detailed bands are cut out there.

- contours (DEPCNT): nodes and ways tagged contour=depth, depth and name as charted (one decimal: US charts are in
  feet, 1.8 m is the 6 ft line - rounding it to 2 would read deeper than charted), contourtype of the nearest level
  of rendering_types within 10 % so that the style shows it at the usual zooms (1.8 -> 2m, 9.1 -> 10m, 18.2 -> 20m);
  the levels with no such type (0.9, 3.6 m...) get contourtype=minor and go to --minor, meant for a map section of
  zoom 15 and above only
- soundings (SOUNDG): point layer "soundings" with a positive "depth" field, for depth_soundings_osm.py
- areas (DEPARE, DRGARE): polygon layer "areas" with the shallowest depth "mindepth" (DRVAL1), for depth_areas_osm.py
- coverage: polygon layer "coverage" of all read cells, to leave the gridded contours out there (-x/-X)
"""
import argparse
import glob
import gzip
import os

from osgeo import gdal, ogr, osr

gdal.UseExceptions()
ogr.UseExceptions()
gdal.SetConfigOption('OGR_S57_OPTIONS', 'SPLIT_MULTIPOINT=ON,ADD_SOUNDG_DEPTH=ON,RETURN_PRIMITIVES=OFF')
CONTOUR_TYPES = [2, 5, 10, 20, 50, 100, 200, 1000]


def contourtype(depth):
    best = min(CONTOUR_TYPES, key=lambda t: abs(t - depth))
    return '%dm' % best if abs(best - depth) <= 0.1 * best else None


def label(depth):
    return '%d' % depth if depth == int(depth) else '%.1f' % depth


def polygons(g):
    """Only the polygon parts: an intersection with the box may add the lines and points of touching edges."""
    out = ogr.Geometry(ogr.wkbMultiPolygon)
    parts = [g.GetGeometryRef(i) for i in range(g.GetGeometryCount())] if g.GetGeometryCount() and \
        ogr.GT_Flatten(g.GetGeometryType()) in (ogr.wkbMultiPolygon, ogr.wkbGeometryCollection) else [g]
    for part in parts:
        if ogr.GT_Flatten(part.GetGeometryType()) == ogr.wkbPolygon:
            out.AddGeometry(part)
        elif ogr.GT_Flatten(part.GetGeometryType()) == ogr.wkbMultiPolygon:
            for i in range(part.GetGeometryCount()):
                out.AddGeometry(part.GetGeometryRef(i))
    return out


def cells(paths, bands):
    found = []
    for p in paths:
        files = [p] if p.endswith('.000') else glob.glob(os.path.join(p, '**', '*.000'), recursive=True)
        for f in files:
            name = os.path.basename(f)
            if len(name) > 2 and name[2] in bands:
                found.append((int(name[2]), f))
    return sorted(found)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('cells', nargs='+')
    parser.add_argument('--bbox', nargs=4, type=float, required=True, metavar=('W', 'S', 'E', 'N'))
    parser.add_argument('--contours', required=True)
    parser.add_argument('--minor', required=True, help='contours of the levels without a contourtype')
    parser.add_argument('--soundings', required=True)
    parser.add_argument('--areas', required=True)
    parser.add_argument('--coverage', required=True)
    parser.add_argument('--bands', default='4,5,6')
    parser.add_argument('--first-id', type=int, default=500000000)
    args = parser.parse_args()
    w, s, e, n = args.bbox
    box = ogr.CreateGeometryFromWkt('POLYGON((%f %f,%f %f,%f %f,%f %f,%f %f))' % (w, s, e, s, e, n, w, n, w, s))
    bands = set(args.bands.split(','))

    # coverage of every cell touching the box, per band
    used = []
    for band, f in cells(args.cells, bands):
        ds = ogr.Open(f)
        lyr = ds.GetLayerByName('M_COVR')
        if lyr is None:
            continue
        x0, x1, y0, y1 = lyr.GetExtent()
        if x1 < w or x0 > e or y1 < s or y0 > n:
            continue
        cov = ogr.Geometry(ogr.wkbMultiPolygon)
        for feat in lyr:
            g = feat.GetGeometryRef()
            if feat.GetField('CATCOV') == 1 and g is not None:
                cov = cov.Union(g)
        cov = polygons(cov.Intersection(box))
        if not cov.IsEmpty():
            used.append((band, f, cov))
    print('ENC cells in the box: %d (%s)' % (len(used), ', '.join(
        '%d x band %d' % (sum(1 for u in used if u[0] == b), b) for b in sorted({u[0] for u in used}))))

    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    gpkg = ogr.GetDriverByName('GPKG')
    for path in (args.coverage, args.soundings, args.areas):
        if os.path.exists(path):
            os.remove(path)
    cov_ds = gpkg.CreateDataSource(args.coverage)
    cov_lyr = cov_ds.CreateLayer('coverage', srs, ogr.wkbMultiPolygon)
    snd_ds = gpkg.CreateDataSource(args.soundings)
    snd_lyr = snd_ds.CreateLayer('soundings', srs, ogr.wkbPoint)
    snd_lyr.CreateField(ogr.FieldDefn('depth', ogr.OFTReal))
    area_ds = gpkg.CreateDataSource(args.areas)
    area_lyr = area_ds.CreateLayer('areas', srs, ogr.wkbMultiPolygon)
    area_lyr.CreateField(ogr.FieldDefn('mindepth', ogr.OFTReal))

    node_id = args.first_id
    ways = soundings = areas = 0
    snd_lyr.StartTransaction()
    area_lyr.StartTransaction()
    header = "<?xml version='1.0' encoding='UTF-8'?>\n<osm version='0.6' upload='false' generator='depth_enc_osm'>\n"
    with gzip.open(args.contours, 'wt', compresslevel=5) as major, gzip.open(args.minor, 'wt', compresslevel=5) as minor:
        major.write(header)
        minor.write(header)
        for band, f, cov in used:
            feat = ogr.Feature(cov_lyr.GetLayerDefn())
            feat.SetGeometry(ogr.ForceToMultiPolygon(cov))
            cov_lyr.CreateFeature(feat)
            # the area of this cell that no more detailed cell covers
            own = cov
            for b2, _, cov2 in used:
                if b2 > band and own.Intersects(cov2):
                    own = own.Difference(cov2)
            if own.IsEmpty():
                continue
            ds = ogr.Open(f)
            lyr = ds.GetLayerByName('DEPCNT')
            for feat in lyr or []:
                depth, g = feat.GetField('VALDCO'), feat.GetGeometryRef()
                if depth is None or depth <= 0 or g is None:
                    continue
                g = g.Intersection(own)
                parts = [g.GetGeometryRef(i) for i in range(g.GetGeometryCount())] if g.GetGeometryCount() else [g]
                tags = '<tag k="contour" v="depth"/><tag k="depth" v="%s"/><tag k="name" v="%s"/>' % (label(depth), label(depth))
                ct = contourtype(depth)
                tags += '<tag k="contourtype" v="%s"/>' % (ct or 'minor')
                out = major if ct else minor
                for part in parts:
                    if part.GetGeometryType() not in (ogr.wkbLineString, ogr.wkbLineString25D) or part.GetPointCount() < 2:
                        continue
                    refs = []
                    for i in range(part.GetPointCount()):
                        node_id += 1
                        out.write('<node id="-%d" visible="true" lat="%.7f" lon="%.7f"/>\n' % (node_id, part.GetY(i), part.GetX(i)))
                        refs.append(node_id)
                    node_id += 1
                    ways += 1
                    out.write('<way id="-%d" visible="true">%s%s</way>\n' % (
                        node_id, ''.join('<nd ref="-%d"/>' % r for r in refs), tags))
            for name in ('DEPARE', 'DRGARE'):
                for feat in ds.GetLayerByName(name) or []:
                    depth, g = feat.GetField('DRVAL1'), feat.GetGeometryRef()
                    if depth is None or g is None or ogr.GT_Flatten(g.GetGeometryType()) not in (ogr.wkbPolygon, ogr.wkbMultiPolygon):
                        continue
                    g = polygons(g.Intersection(own))
                    if g.IsEmpty():
                        continue
                    a = ogr.Feature(area_lyr.GetLayerDefn())
                    a.SetField('mindepth', depth)
                    a.SetGeometry(g)
                    area_lyr.CreateFeature(a)
                    areas += 1
            lyr = ds.GetLayerByName('SOUNDG')
            for feat in lyr or []:
                depth, g = feat.GetField('DEPTH'), feat.GetGeometryRef()
                if depth is None or g is None or not own.Contains(g):
                    continue
                pt = ogr.Feature(snd_lyr.GetLayerDefn())
                pt.SetField('depth', depth)
                pt.SetGeometry(ogr.CreateGeometryFromWkt('POINT(%f %f)' % (g.GetX(), g.GetY())))
                snd_lyr.CreateFeature(pt)
                soundings += 1
        major.write('</osm>\n')
        minor.write('</osm>\n')
    snd_lyr.CommitTransaction()
    area_lyr.CommitTransaction()
    print('ENC contours: %d ways, soundings: %d, depth areas: %d' % (ways, soundings, areas))


if __name__ == '__main__':
    main()
