#!/usr/bin/env python3
"""Clean depth contours made by gdal_contour before they go to OSM.

    depth_contours_filter.py INPUT OUTPUT --cell DEGREES [--min-ring-cells N]

INPUT has line features (lon/lat) with an `elev` field (metres, negative below sea level). OUTPUT (GeoPackage) keeps
the lines below 0 m with a `depth` field (positive metres), drops closed rings shorter than N grid cells -
single cells and noise on flat shelves - and simplifies every line by half a grid cell.
"""
import argparse
import math

from osgeo import ogr

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
    parser.add_argument('--min-ring-cells', type=float, default=8,
                        help='closed rings shorter than this many cells are dropped')
    args = parser.parse_args()

    src = ogr.Open(args.input)
    layer = src.GetLayer(0)
    out = ogr.GetDriverByName('GPKG').CreateDataSource(args.output)
    # lon/lat of EPSG:4326, written without a spatial reference on purpose: ogr2osm reprojects a layer that has one
    # to EPSG:4326 in its authority axis order under GDAL 3 and swaps latitude and longitude
    out_layer = out.CreateLayer('depth_contours', None, ogr.wkbLineString)
    out_layer.CreateField(ogr.FieldDefn('depth', ogr.OFTInteger))

    min_ring_m = args.min_ring_cells * args.cell * 111320
    kept = dropped = 0
    out.StartTransaction()
    for feature in layer:
        elev = feature.GetField('elev')
        geom = feature.GetGeometryRef()
        if elev is None or elev >= 0 or geom is None:
            continue
        parts = [geom] if geom.GetGeometryType() in (ogr.wkbLineString, ogr.wkbLineString25D) \
            else [geom.GetGeometryRef(i) for i in range(geom.GetGeometryCount())]
        for part in parts:
            closed = part.GetPointCount() > 2 and part.GetPoint_2D(0) == part.GetPoint_2D(part.GetPointCount() - 1)
            if closed and length_m(part) < min_ring_m:
                dropped += 1
                continue
            simple = part.SimplifyPreserveTopology(args.cell / 2)
            if simple is None or simple.IsEmpty() or simple.GetPointCount() < 2:
                dropped += 1
                continue
            f = ogr.Feature(out_layer.GetLayerDefn())
            f.SetField('depth', int(round(-elev)))
            f.SetGeometry(simple)
            out_layer.CreateFeature(f)
            kept += 1
    out.CommitTransaction()
    print('contours kept %d, dropped %d (rings shorter than %.0f m or degenerate)' % (kept, dropped, min_ring_m))


if __name__ == '__main__':
    main()
