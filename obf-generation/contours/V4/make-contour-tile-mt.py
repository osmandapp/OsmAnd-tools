#!/usr/bin/env python3
"""
Multithreaded tiff tile to contour (osm) converter for OsmAnd
Requires: gdal, lbzip2, qgis
"""

import os
import re
import sys
import argparse
import tempfile
import shutil
import random
import subprocess
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

# Setup QGIS paths before importing
sys.path.append('/usr/share/qgis/python/plugins/')
os.environ['QT_QPA_PLATFORM'] = 'offscreen'
os.environ['QT_LOGGING_RULES'] = 'qt5ct.debug=false'

from qgis.core import QgsApplication, QgsFeatureRequest
from qgis.analysis import QgsNativeAlgorithms
import processing
from processing.core.Processing import Processing
from processing.tools import dataobjects

# Initialize QGIS once
QgsApplication.setPrefixPath('/usr', True)
qgs = QgsApplication([], False)
qgs.initQgis()
QgsApplication.processingRegistry().addProvider(QgsNativeAlgorithms())
Processing.initialize()
print("QGIS initialized")


def run_qgis_algorithm(alg, params):
    """Run QGIS algorithm"""
    print(f"\033[95mAlgorithm: {alg}\033[0m")
    for k, v in params.items():
        if v is not None:
            print(f"{k}: {v}")

    context = dataobjects.createContext()
    context.setInvalidGeometryCheck(QgsFeatureRequest.GeometryNoCheck)
    return processing.run(alg, params, context=context)


def parse_tile_range(range_str):
    """Parse tile range like N30-56E003-025 or N40-41W108-E001"""
    if not range_str:
        return None

    pattern1 = r'^([NS])(\d+)-([NS])(\d+)([EW])(\d+)-([EW])(\d+)$'
    pattern2 = r'^([NS])(\d+)-(\d+)([EW])(\d+)-(\d+)$'

    m = re.match(pattern1, range_str)
    if m:
        lat_p1, lat1, lat_p2, lat2, lon_p1, lon1, lon_p2, lon2 = m.groups()
    else:
        m = re.match(pattern2, range_str)
        if m:
            lat_p1, lat1, lat2, lon_p1, lon1, lon2 = m.groups()
            lat_p2, lon_p2 = lat_p1, lon_p1
        else:
            raise ValueError(f"Invalid tile range: {range_str}")

    lat1 = int(lat1) if lat_p1 == 'N' else -int(lat1)
    lat2 = int(lat2) if lat_p2 == 'N' else -int(lat2)
    lon1 = int(lon1) if lon_p1 == 'E' else -int(lon1)
    lon2 = int(lon2) if lon_p2 == 'E' else -int(lon2)

    return {
        'lat_min': min(lat1, lat2), 'lat_max': max(lat1, lat2),
        'lon_min': min(lon1, lon2), 'lon_max': max(lon1, lon2)
    }


def in_range(filename, rng):
    """Check if tile filename is within range"""
    if not rng:
        return True
    m = re.match(r'^([NS])(\d+)([EW])(\d+)$', filename)
    if not m:
        return True
    lat_p, lat, lon_p, lon = m.groups()
    lat = int(lat) if lat_p == 'N' else -int(lat)
    lon = int(lon) if lon_p == 'E' else -int(lon)
    return (rng['lat_min'] <= lat <= rng['lat_max'] and
            rng['lon_min'] <= lon <= rng['lon_max'])


def get_neighbor_filename(base_dir, highres_dir, lat_p, lat, lon_p, lon, lat_digits, lon_digits):
    """Generate neighbor tile filename and check existence"""
    fname = f"{lat_p}{lat:0{lat_digits}d}{lon_p}{lon:0{lon_digits}d}.tif"
    for d in [highres_dir, base_dir]:
        if d and Path(d).exists():
            p = Path(d) / fname
            if p.exists():
                return str(p)
    return None


def process_tile(tiff_path, args_dict):
    """Process single tile"""
    filename = Path(tiff_path).stem
    out_file = Path(args_dict['outdir']) / f"{filename}.osm.bz2"

    if out_file.exists() and not args_dict['reprocess']:
        print(f"Skipping {tiff_path} (already processed)")
        return 0

    if out_file.exists():
        out_file.unlink()

    print(f"\n{'=' * 50}\nProcessing {filename}\n{'=' * 50}")

    # Parse tile coordinates
    m = re.match(r'^([NS])(\d+)([EW])(\d+)$', filename)
    if not m:
        print(f"Warning: Invalid filename format: {filename}, skipping")
        return 0

    lat_p, lat, lon_p, lon = m.groups()
    lat_digits, lon_digits = len(lat), len(lon)
    num_lat = int(lat) if lat_p == 'N' else -int(lat)
    num_lon = int(lon) if lon_p == 'E' else -int(lon)

    # Find neighbors
    neighbors = []
    indir = Path(tiff_path).parent
    highres_dir = indir.parent / f"{indir.name}_highres" if indir.parent else None
    neighbors_dir = Path(args_dict.get('neighbors_dir', args_dict['indir']))
    neighbors_highres_dir = neighbors_dir.parent / f"{neighbors_dir.name}_highres"

    # Track which neighbors exist for bounds calculation
    neighbor_exists = {'left': False, 'right': False, 'top': False, 'bottom': False}

    for dlat in [-1, 0, 1]:
        for dlon in [-1, 0, 1]:
            if dlat == 0 and dlon == 0:
                neighbors.append(str(tiff_path))
                continue

            new_lat = int(lat) + dlat
            new_lon = int(lon) + dlon
            new_lat_p, new_lon_p = lat_p, lon_p

            # Handle hemisphere crossings
            if lat_p == 'N' and new_lat == -1:
                new_lat_p, new_lat = 'S', 1
            elif lat_p == 'S' and new_lat == 0:
                new_lat_p, new_lat = 'N', 0

            if lon_p == 'E' and new_lon == -1:
                new_lon_p, new_lon = 'W', 1
            elif lon_p == 'W' and new_lon == 0:
                new_lon_p, new_lon = 'E', 0

            # Check all possible locations
            nf = None
            for check_dir in [highres_dir, indir, neighbors_highres_dir, neighbors_dir]:
                nf = get_neighbor_filename(check_dir, None, new_lat_p, new_lat,
                                           new_lon_p, new_lon, lat_digits, lon_digits)
                if nf:
                    break

            if nf:
                neighbors.append(nf)
                if dlat == 0 and dlon == -1:
                    neighbor_exists['left' if lon_p == 'E' else 'right'] = True
                elif dlat == 0 and dlon == 1:
                    neighbor_exists['right' if lon_p == 'E' else 'left'] = True
                elif dlat == -1 and dlon == 0:
                    neighbor_exists['bottom' if lat_p == 'N' else 'top'] = True
                elif dlat == 1 and dlon == 0:
                    neighbor_exists['top' if lat_p == 'N' else 'bottom'] = True

    print(f"Found {len(neighbors)} neighbors")

    # Process with GDAL
    with tempfile.TemporaryDirectory(dir=args_dict['tmp_dir']) as tmp:
        merged = f"{tmp}/merged_{filename}.tif"
        xres = 0.0002776235424764020234

        # Merge neighbors
        subprocess.run(['gdalwarp', '-overwrite', '-t_srs', 'EPSG:4326',
                        '-tr', str(xres), str(xres), '-tap', '-ot', 'Int16',
                        '-of', 'GTiff', '-co', 'COMPRESS=LZW'] + neighbors + [merged],
                       check=True, capture_output=True)

        # Get bounds from original tile
        info = subprocess.check_output(['gdalinfo', tiff_path]).decode()
        ul_match = re.search(r'Upper Left\s*\(\s*([-\d.]+),\s*([-\d.]+)\)', info)
        if not ul_match:
            raise ValueError(f"Cannot get bounds for {tiff_path}")
        ulx, uly = float(ul_match.group(1)), float(ul_match.group(2))

        # Calculate expanded bounds
        delta = 0.03 * 0.5

        new_xmin = ulx - delta if neighbor_exists['left'] else ulx
        new_xmax = ulx + 1 + (delta if neighbor_exists['right'] else 0)
        new_ymax = uly + (delta if neighbor_exists['top'] else 0)
        new_ymin = uly - 1 - (delta if neighbor_exists['bottom'] else 0)

        # Clip to expanded bounds
        clipped = f"{tmp}/{filename}.tif"
        subprocess.run(['gdalwarp', '-overwrite', '-t_srs', 'EPSG:4326',
                        '-tr', str(xres), str(xres), '-tap', '-ot', 'Int16',
                        '-of', 'GTiff', '-co', 'COMPRESS=LZW',
                        '-te', str(new_xmin), str(new_ymin), str(new_xmax), str(new_ymax),
                        merged, clipped], check=True, capture_output=True)

        # Convert to feet if needed
        src = clipped
        if args_dict.get('feet'):
            feet_tif = f"{tmp}/{filename}_ft.tif"
            subprocess.run(['gdal_calc.py', '-A', src, '--outfile', feet_tif,
                            '--calc=A/0.3048', '--quiet'], check=True)
            src = feet_tif

        # Smooth if enabled
        if args_dict.get('smooth'):
            no_smooth = False
            ini_path = Path(args_dict['working_dir']) / 'no_smoothing.ini'
            if ini_path.exists():
                with open(ini_path) as f:
                    if filename in f.read():
                        no_smooth = True
                        print("No smoothing applied (in no_smoothing.ini)")

            if not no_smooth:
                smooth_tif = f"{tmp}/{filename}_smooth.tif"
                if int(lat) >= 65:
                    info = subprocess.check_output(['gdalinfo', src]).decode()
                    size_match = re.search(r'Size is (\d+), (\d+)', info)
                    if size_match:
                        width, height = int(size_match.group(1)), int(size_match.group(2))
                        temp_smooth = f"{tmp}/temp_smooth.tif"
                        subprocess.run(['gdalwarp', '-overwrite', '-ts', str(width // 2), str(height // 2),
                                        '-r', 'cubicspline', '-co', 'COMPRESS=LZW', '-ot', 'Float32',
                                        '-wo', 'NUM_THREADS=4', '-multi', src, temp_smooth], check=True)
                        subprocess.run(['gdalwarp', '-overwrite', '-ts', str(width), str(height),
                                        '-of', 'GTiff', '-r', 'cubicspline', '-co', 'COMPRESS=LZW',
                                        '-ot', 'Float32', '-wo', 'NUM_THREADS=4', '-multi',
                                        temp_smooth, smooth_tif], check=True)
                else:
                    subprocess.run(['gdalwarp', '-overwrite', '-r', 'cubicspline',
                                    '-co', 'COMPRESS=LZW', '-ot', 'Float32',
                                    '-wo', 'NUM_THREADS=4', '-multi', src, smooth_tif], check=True)
                src = smooth_tif
                print("Smoothing applied")

        # Generate contours
        step = 40 if args_dict.get('feet') else 10
        info = subprocess.check_output(['gdalinfo', src]).decode()
        size_match = re.search(r'Size is (\d+), (\d+)', info)
        if size_match and int(size_match.group(1)) >= 30000:
            step = 20 if args_dict.get('feet') else 5

        print(f"Using isolines_step={step}")
        shp = f"{tmp}/{filename}.shp"
        subprocess.run(['gdal_contour', '-i', str(step), '-a', 'height', src, shp],
                       check=True, capture_output=True)

        # Simplify geometries
        if args_dict.get('simplify'):
            print("Simplifying lines...")
            shp_simp = f"{tmp}/{filename}_simplified.shp"
            run_qgis_algorithm('native:simplifygeometries', {
                'INPUT': shp,
                'METHOD': 0,
                'TOLERANCE': 1e-05,
                'OUTPUT': shp_simp
            })
            for ext in ['.shp', '.dbf', '.prj', '.shx']:
                src_file = Path(str(shp_simp).replace('.shp', ext))
                dst_file = Path(shp).with_suffix(ext)
                if src_file.exists():
                    shutil.move(str(src_file), str(dst_file))

        # Split lines
        if args_dict.get('split'):
            print("Splitting lines by length...")
            shp_split = f"{tmp}/{filename}_split.shp"
            run_qgis_algorithm('native:splitlinesbylength', {
                'INPUT': shp,
                'LENGTH': 0.05,
                'OUTPUT': shp_split
            })
            for ext in ['.shp', '.dbf', '.prj', '.shx']:
                src_file = Path(str(shp_split).replace('.shp', ext))
                dst_file = Path(shp).with_suffix(ext)
                if src_file.exists():
                    shutil.move(str(src_file), str(dst_file))

        # Clip to exact tile bounds
        print("Cropping by cutline...")
        geojson = f"{tmp}/bbox.geojson"
        lat_min, lat_max = num_lat, num_lat + 1
        lon_min, lon_max = num_lon, num_lon + 1
        with open(geojson, 'w') as f:
            f.write(
                f'''{{"type":"FeatureCollection","crs":{{"type":"name","properties":{{"name":"EPSG:4326"}}}},"features":[{{"type":"Feature","geometry":{{"type":"Polygon","coordinates":[[[{lon_min},{lat_min}],[{lon_max},{lat_min}],[{lon_max},{lat_max}],[{lon_min},{lat_max}],[{lon_min},{lat_min}]]]}}}}]}}''')

        shp_cut = f"{tmp}/{filename}_cut.shp"
        run_qgis_algorithm('native:clip', {
            'INPUT': shp,
            'OVERLAY': geojson,
            'OUTPUT': shp_cut
        })
        for ext in ['.shp', '.dbf', '.prj', '.shx']:
            src_file = Path(str(shp_cut).replace('.shp', ext))
            dst_file = Path(shp).with_suffix(ext)
            if src_file.exists():
                shutil.move(str(src_file), str(dst_file))

        # Convert to OSM
        print("Building osm file...")
        osm = f"{args_dict['outdir']}/{filename}.osm"
        script = 'contours_feet.py' if args_dict.get('feet') else 'contours.py'
        subprocess.run([f"{args_dict['working_dir']}/ogr2osm.py", shp, '-o', osm,
                        '-e', '4326', '-t', script], check=True)

        # Compress
        print("Compressing to osm.bz2...")
        subprocess.run(['lbzip2', '-f', osm], check=True)

        print(f"Successfully processed {filename}")

    return 1


def main():
    parser = argparse.ArgumentParser(description='Convert TIFF tiles to contour OSM files')
    parser.add_argument('-i', '--indir', required=True)
    parser.add_argument('-o', '--outdir', required=True)
    parser.add_argument('-m', '--tmp-dir', default='/tmp/contours')
    parser.add_argument('-s', '--smooth', action='store_true')
    parser.add_argument('-p', '--split', action='store_true')
    parser.add_argument('-d', '--simplify', action='store_true')
    parser.add_argument('-f', '--feet', action='store_true')
    parser.add_argument('-t', '--threads', type=int, default=1)
    parser.add_argument('-c', '--cutline')
    parser.add_argument('-n', '--neighbors-dir')
    parser.add_argument('-r', '--reprocess', action='store_true')
    parser.add_argument('-R', '--range')

    args = parser.parse_args()

    print(f"Input dir: {args.indir}")
    print(f"Output dir: {args.outdir}")
    print(f"Smooth: {args.smooth}")
    print(f"Simplify: {args.simplify}")
    print(f"Split lines: {args.split}")
    print(f"Feet: {args.feet}")
    print(f"Reprocess: {args.reprocess}")
    if args.range:
        print(f"Tile range: {args.range}")

    # Prepare directories
    Path(args.outdir).mkdir(parents=True, exist_ok=True)
    Path(args.tmp_dir).mkdir(parents=True, exist_ok=True)

    # Parse range
    tile_range = parse_tile_range(args.range) if args.range else None
    if tile_range:
        print(f"Range: lat {tile_range['lat_min']}..{tile_range['lat_max']}, "
              f"lon {tile_range['lon_min']}..{tile_range['lon_max']}")

    # Find tiff files
    tiffs = list(Path(args.indir).glob("*.tif"))
    if tile_range:
        tiffs = [t for t in tiffs if in_range(t.stem, tile_range)]

    print(f"Found {len(tiffs)} tiles to process")

    if not tiffs:
        print("No tiles to process")
        return

    random.shuffle(tiffs)

    args_dict = {
        'outdir': args.outdir,
        'tmp_dir': args.tmp_dir,
        'smooth': args.smooth,
        'split': args.split,
        'simplify': args.simplify,
        'feet': args.feet,
        'reprocess': args.reprocess,
        'indir': args.indir,
        'neighbors_dir': args.neighbors_dir or args.indir,
        'working_dir': os.getcwd()
    }

    processed = 0
    if args.threads > 1:
        with ProcessPoolExecutor(max_workers=args.threads) as executor:
            futures = [executor.submit(process_tile, str(t), args_dict) for t in tiffs]
            for f in futures:
                processed += f.result()
    else:
        for tiff in tiffs:
            processed += process_tile(str(tiff), args_dict)

    print(f"\n{'=' * 50}")
    print(f"Success! Processed tiles: {processed}")
    print(f"{'=' * 50}")


if __name__ == '__main__':
    main()
