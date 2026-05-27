#!/usr/bin/env python3
import os
import sys
import argparse
from osgeo import gdal, ogr
import xml.etree.ElementTree as ET
from rtree import index
import subprocess


def get_tile_geometry_from_file(tiff_path):
    """Gets tile geometry from TIFF file"""
    try:
        ds = gdal.Open(tiff_path, gdal.GA_ReadOnly)
        if not ds:
            return None

        geo_transform = ds.GetGeoTransform()
        x_size = ds.RasterXSize
        y_size = ds.RasterYSize

        ulx = geo_transform[0]
        uly = geo_transform[3]
        lrx = ulx + geo_transform[1] * x_size
        lry = uly + geo_transform[5] * y_size

        ring = ogr.Geometry(ogr.wkbLinearRing)
        ring.AddPoint(ulx, uly)
        ring.AddPoint(lrx, uly)
        ring.AddPoint(lrx, lry)
        ring.AddPoint(ulx, lry)
        ring.AddPoint(ulx, uly)

        polygon = ogr.Geometry(ogr.wkbPolygon)
        polygon.AddGeometry(ring)

        ds = None
        return polygon

    except Exception:
        return None


def create_tile_index_from_vrt(vrt_file):
    """Creates spatial index for tiles from VRT file"""
    tile_index = index.Index()
    tile_geometries = {}
    tile_id = 0

    vrt_tree = ET.parse(vrt_file)
    root = vrt_tree.getroot()

    for vrtrasterband in root.findall(".//VRTRasterBand"):
        sources = vrtrasterband.findall(".//SimpleSource") + vrtrasterband.findall(".//ComplexSource")

        for source in sources:
            filename_elem = source.find("SourceFilename")
            if filename_elem is None:
                continue

            filename = filename_elem.text
            full_path = os.path.join(os.path.dirname(vrt_file), filename)

            if not os.path.exists(full_path):
                continue

            polygon = get_tile_geometry_from_file(full_path)
            if not polygon:
                continue

            bbox = polygon.GetEnvelope()
            correct_bbox = (bbox[0], bbox[2], bbox[1], bbox[3])

            tile_index.insert(tile_id, correct_bbox)
            tile_geometries[tile_id] = polygon
            tile_id += 1

    print(f"Loaded tiles: {len(tile_geometries)}")
    return tile_index, tile_geometries


def convert_osm_to_geojson(osm_file, geojson_file):
    """Converts OSM file to GeoJSON"""
    try:
        result = subprocess.run(['which', 'osmtogeojson'], capture_output=True, text=True)
        if result.returncode != 0:
            print("osmtogeojson not found. Install: npm install -g osmtogeojson")
            return False

        with open(geojson_file, 'w') as outfile:
            subprocess.run(['osmtogeojson', osm_file], stdout=outfile, check=True)
        return True

    except Exception as e:
        print(f"Conversion error: {e}")
        return False


def geometry_from_geojson(geojson_geom):
    """Creates OGR geometry from GeoJSON, handling LineString as polygons if they are closed"""
    geom_type = geojson_geom['type']

    if geom_type == 'Polygon':
        return ogr.CreateGeometryFromJson(str(geojson_geom))

    elif geom_type == 'LineString':
        coordinates = geojson_geom['coordinates']
        if len(coordinates) >= 4 and coordinates[0] == coordinates[-1]:
            ring = ogr.Geometry(ogr.wkbLinearRing)
            for coord in coordinates:
                ring.AddPoint(coord[0], coord[1])

            polygon = ogr.Geometry(ogr.wkbPolygon)
            polygon.AddGeometry(ring)
            return polygon

    elif geom_type == 'MultiPolygon':
        return ogr.CreateGeometryFromJson(str(geojson_geom))

    return None


def find_overlapping_regions(geojson_file, tile_index, tile_geometries):
    """Finds regions from GeoJSON that intersect with tiles"""
    overlapping_regions = set()

    import fiona
    with fiona.open(geojson_file, 'r') as src:
        for feature in src:
            geom = geometry_from_geojson(feature['geometry'])
            if not geom:
                continue

            bbox = geom.GetEnvelope()
            correct_bbox = (bbox[0], bbox[2], bbox[1], bbox[3])
            possible_tiles = list(tile_index.intersection(correct_bbox))

            for tile_id in possible_tiles:
                if geom.Intersects(tile_geometries[tile_id]):
                    properties = feature['properties']
                    region_name = properties.get('region_full_name') or properties.get('name')
                    if region_name:
                        overlapping_regions.add(region_name)
                    break

    return sorted(list(overlapping_regions))


def write_output(regions, output_file=None, delimiter=','):
    """Writes regions to file or prints to screen with specified delimiter"""
    if output_file:
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                if delimiter == '\n':
                    for region in regions:
                        f.write(f"{region}\n")
                else:
                    f.write(delimiter.join(regions))
                    if regions:
                        f.write('\n')
            print(f"Result written to file: {output_file}")
        except Exception as e:
            print(f"Error writing to file: {e}")
            return False
    else:
        # Print to screen
        print("\nOverlapping regions:")
        print("=" * 40)
        if delimiter == '\n':
            for region in regions:
                print(region)
        else:
            print(delimiter.join(regions))
    
    return True


def main():
    parser = argparse.ArgumentParser(
        description='Find OSM regions overlapping with VRT data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Usage examples:
  %(prog)s --vrt data.vrt --osm regions.osm
  %(prog)s --vrt data.vrt --osm regions.osm --output regions.txt --delimiter ", "
  %(prog)s --vrt data.vrt --osm regions.osm -o regions.txt -d "\\n"
        """
    )
    
    parser.add_argument('--vrt', required=True, help='Path to VRT file')
    parser.add_argument('--osm', required=True, help='Path to OSM file')
    parser.add_argument('--tmp', default='/tmp/regions_temp.geojson', help='Temporary GeoJSON file')
    parser.add_argument('-o', '--output', dest='output', help='File to save results')
    parser.add_argument('-d', '--delimiter', dest='delimiter', default='\n', 
                       help='Delimiter for output (default: newline)')

    # Show help if no arguments provided
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    try:
        args = parser.parse_args()
    except SystemExit as e:
        if e.code != 0:
            print("\nCommand line argument error!")
            print("Check syntax. Example of correct usage:")
            print("python3 get_osmand_regions_from_vrt.py --vrt file.vrt --osm file.osm -o out.csv -d ,")
        sys.exit(1)

    # Process delimiter
    delimiter = args.delimiter
    if delimiter == '\\n':
        delimiter = '\n'
    elif delimiter == '\\t':
        delimiter = '\t'
    elif delimiter == '\\r':
        delimiter = '\r'

    if not os.path.exists(args.vrt):
        print(f"Error: VRT file does not exist: {args.vrt}")
        sys.exit(1)

    if not os.path.exists(args.osm):
        print(f"Error: OSM file does not exist: {args.osm}")
        sys.exit(1)

    print("Creating tile index...")
    tile_index, tile_geometries = create_tile_index_from_vrt(args.vrt)

    if len(tile_geometries) == 0:
        print("No tiles with data found")
        sys.exit(1)

    print("Converting OSM to GeoJSON...")
    if not convert_osm_to_geojson(args.osm, args.tmp):
        print("Error converting OSM file")
        sys.exit(1)

    print("Searching for overlapping regions...")
    overlapping_regions = find_overlapping_regions(args.tmp, tile_index, tile_geometries)

    # Write results
    write_output(overlapping_regions, args.output, delimiter)

    # Clean up temporary file
    if os.path.exists(args.tmp):
        os.remove(args.tmp)


if __name__ == "__main__":
    main()