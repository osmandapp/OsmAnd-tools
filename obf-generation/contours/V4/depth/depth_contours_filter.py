#!/usr/bin/env python3
"""Clean depth contours made by gdal_contour before they go to OSM.

    depth_contours_filter.py INPUT OUTPUT --cell DEGREES [--min-ring-cells N] [--bbox W S E N]
                             [--min-line-m M] [--simplify DEGREES] [--simplify-m METRES]

INPUT has line features (lon/lat) with an `elev` field (metres, negative below sea level) or a positive `depth` field
(charted contours). OUTPUT (FlatGeobuf) keeps the lines below 0 m with a `depth` field (positive metres), drops closed
rings shorter than N grid cells - single cells, and the bumps and hollows a multibeam survey leaves on a chart - and
open pieces shorter than MIN_LINE_M. Every vertex is kept unless --simplify gives a tolerance in degrees or
--simplify-m gives one in metres.
"""
import argparse
import math

from osgeo import ogr
from shapely import wkb
from shapely.geometry import LineString, Point
from shapely.ops import nearest_points

ogr.UseExceptions()


def length_m(geom):
    """Length of a lat/lon line in metres, good enough to tell noise from a contour."""
    total = 0.0
    for i in range(1, geom.GetPointCount()):
        lon1, lat1 = geom.GetX(i - 1), geom.GetY(i - 1)
        lon2, lat2 = geom.GetX(i), geom.GetY(i)
        dx = (lon2 - lon1) * math.cos(math.radians((lat1 + lat2) / 2))
        total += math.hypot(dx, lat2 - lat1) * 111320
    return total


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input')
    parser.add_argument('output')
    parser.add_argument('--cell', type=float, required=True, help='grid cell size in degrees')
    parser.add_argument('--bbox', nargs=4, type=float, metavar=('W', 'S', 'E', 'N'),
                        help='cut the lines to this box before simplifying: the grid of a tile reaches past its box, '
                             'so the lines of two tiles meet exactly at their common edge')
    parser.add_argument('--min-line-m', type=float, default=0, help='drop open pieces shorter than this many metres')
    parser.add_argument('--simplify', type=float, default=0, help='simplification tolerance in degrees; 0 keeps every '
                                                                 'vertex and is the default')
    parser.add_argument('--simplify-m', type=float, default=0, help='simplification tolerance in metres, converted to '
                                                                   'degrees with 111320 m per degree (conservative in longitude)')
    parser.add_argument('--min-ring-cells', type=float, default=8,
                        help='closed rings shorter than this many cells are dropped')
    parser.add_argument('--water',
                        help='water outline of the lake: an open contour ending near it is extended to the shore')
    parser.add_argument('--lake', help='value of the lake field in the water layer')
    parser.add_argument('--snap', type=float, default=0,
                        help='extend an open contour ending this many metres from the water outline, metres')
    args = parser.parse_args()

    water = None
    boundary = None
    if args.water and args.snap > 0:
        wds = ogr.Open(args.water)
        wlayer = wds.GetLayer(0)
        has_lake = wlayer.GetLayerDefn().GetFieldIndex('lake') >= 0
        for f in wlayer:
            if args.lake is not None and has_lake and f.GetField('lake') != args.lake:
                continue
            g = f.GetGeometryRef()
            if g is None:
                continue
            q = wkb.loads(bytes(g.ExportToWkb()))
            water = q if water is None else water.union(q)
        if water is not None:
            boundary = water.boundary
    snap_deg = args.snap / 111320.0

    def snap_point(x, y):
        """The boundary point an open contour end is extended to, or None."""
        if boundary is None:
            return None
        p = Point(x, y)
        if boundary.distance(p) > snap_deg:
            return None
        q = nearest_points(boundary, p)[0]
        if q.distance(p) <= 0 or not water.covers(LineString([p, q])):
            return None
        return q.x, q.y

    src = ogr.Open(args.input)
    layer = src.GetLayer(0)
    out = ogr.GetDriverByName('FlatGeobuf').CreateDataSource(args.output)
    # lon/lat of EPSG:4326, written without a spatial reference on purpose: ogr2osm reprojects a layer that has one
    # to EPSG:4326 in its authority axis order under GDAL 3 and swaps latitude and longitude. Not GeoPackage: it gives
    # a layer without one an "Undefined geographic SRS", which ogr2osm reprojects the same way
    out_layer = out.CreateLayer('depth_contours', None, ogr.wkbLineString)
    out_layer.CreateField(ogr.FieldDefn('depth', ogr.OFTInteger))

    box = None
    if args.bbox:
        w, s, e, n = args.bbox
        box = ogr.CreateGeometryFromWkt('POLYGON((%r %r,%r %r,%r %r,%r %r,%r %r))' % (w, s, e, s, e, n, w, n, w, s))
    min_ring_m = args.min_ring_cells * args.cell * 111320
    tolerance = args.simplify
    if args.simplify_m > 0:
        # one degree of longitude is shorter than 111320 m, so this keeps the tolerance below the requested metres
        tolerance = max(tolerance, args.simplify_m / 111320.0)
    kept = dropped = 0
    fields = [layer.GetLayerDefn().GetFieldDefn(i).GetName() for i in range(layer.GetLayerDefn().GetFieldCount())]
    depth_field = 'elev' if 'elev' in fields else 'depth'
    for feature in layer:
        elev = feature.GetField(depth_field)
        if depth_field == 'depth' and elev is not None:
            elev = -elev
        geom = feature.GetGeometryRef()
        if elev is None or elev >= 0 or geom is None:
            continue
        if box is not None:
            geom = geom.Intersection(box)
            if geom is None or geom.IsEmpty():
                continue
        parts = [geom] if ogr.GT_Flatten(geom.GetGeometryType()) == ogr.wkbLineString \
            else [geom.GetGeometryRef(i) for i in range(geom.GetGeometryCount())
                  if ogr.GT_Flatten(geom.GetGeometryRef(i).GetGeometryType()) == ogr.wkbLineString]
        for part in parts:
            closed = part.GetPointCount() > 2 and part.GetPoint_2D(0) == part.GetPoint_2D(part.GetPointCount() - 1)
            if closed and length_m(part) < min_ring_m:
                dropped += 1
                continue
            if not closed and length_m(part) < args.min_line_m:
                dropped += 1
                continue
            simple = part if tolerance == 0 else part.SimplifyPreserveTopology(tolerance)
            if simple is None or simple.IsEmpty() or simple.GetPointCount() < 2:
                dropped += 1
                continue
            npts = simple.GetPointCount()
            snap_start = snap_end = None
            if not closed:
                snap_start = snap_point(simple.GetX(0), simple.GetY(0))
                snap_end = snap_point(simple.GetX(npts - 1), simple.GetY(npts - 1))
            # OSM ways have a 2000-node limit. Adjacent pieces share their end vertex, so splitting does not
            # change the line; a long closed ring becomes several open ways with the same depth.
            for start in range(0, npts - 1, 1999):
                end = min(start + 1999, npts - 1)
                piece = ogr.Geometry(ogr.wkbLineString)
                if start == 0 and snap_start is not None:
                    piece.AddPoint_2D(*snap_start)
                for i in range(start, end + 1):
                    x, y = simple.GetX(i), simple.GetY(i)
                    piece.AddPoint_2D(x, y)
                if end == npts - 1 and snap_end is not None:
                    piece.AddPoint_2D(*snap_end)
                f = ogr.Feature(out_layer.GetLayerDefn())
                f.SetField('depth', int(round(-elev)))
                f.SetGeometry(piece)
                out_layer.CreateFeature(f)
                kept += 1
    out = None
    print('contours kept %d, dropped %d (rings shorter than %.0f m or degenerate)' % (kept, dropped, min_ring_m))


if __name__ == '__main__':
    main()
