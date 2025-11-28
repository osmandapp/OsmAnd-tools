#!/usr/bin/env python3

import os
import re
import subprocess
import requests
from bs4 import BeautifulSoup
import threading
import numpy as np
from queue import Queue
import shutil
import json
from datetime import datetime, timedelta
import argparse
from collections import defaultdict

class FranceDEMProcessor:
    def __init__(self, debug_mode=False, working_dir=None, existing_tileset_dir=None, max_jobs=None):
        # Directory structure configuration
        self.working_dir = working_dir or os.getcwd()
        self.download_dir = os.path.join(self.working_dir, "data")
        self.temp_dir = os.path.join(self.working_dir, "temp")
        self.processed_source_dir = os.path.join(self.working_dir, "processed_source")
        self.coastline_temp_dir = os.path.join(self.temp_dir, "coastline")

        # Output directories for tiles
        self.highres_dir = os.path.join(self.working_dir, "tiles_highres")
        self.lowres_dir = os.path.join(self.working_dir, "tiles")

        # External data sources
        self.existing_tileset_dir = existing_tileset_dir
        self.existing_tileset_highres_dir = existing_tileset_dir + "_highres" if existing_tileset_dir else None

        # Processing parameters
        self.max_jobs = max_jobs if max_jobs is not None else 18
        self.highres_tile_size = 16384
        self.lowres_tile_size = 3600
        self.debug_mode = debug_mode

        # File paths
        self.empty_tiles_cache_file = os.path.join(self.working_dir, "empty_tiles_cache.json")
        self.tiles_to_enhance_file = os.path.join(self.working_dir, "tiles_enhance.csv")
        self.tiles_to_clip_by_land_file = os.path.join(self.working_dir, "tiles_clip_by_land.csv")
        self.tiles_ignore_coastline_mask_file = os.path.join(self.working_dir, "tiles_ignore_coastline_mask.csv")
        self.tiles_ignore_clip_by_land_file = os.path.join(self.working_dir, "tiles_ignore_clip_by_land.csv")
        self.tiles_ignore_enhance_file = os.path.join(self.working_dir, "tiles_ignore_enhance.csv")

        # Data source URLs
        self.url = "https://geoservices.ign.fr/rgealti"

        # Empty tiles cache settings
        self.cache_expiry_hours = 96

        # Data sets
        self.tiles_to_enhance = set()
        self.tiles_to_clip_by_land = set()
        self.tiles_ignore_coastline_mask = set()
        self.tiles_ignore_clip_by_land = set()
        self.tiles_ignore_enhance = set()
        self.land_polygons_path = None
        self.coastline_path = None

        # Create directories
        self._create_directories()

    def _create_directories(self):
        """Create all necessary directories"""
        directories = [
            self.download_dir,
            self.temp_dir,
            self.processed_source_dir,
            self.coastline_temp_dir,
            self.highres_dir,
            self.lowres_dir
        ]

        for directory in directories:
            os.makedirs(directory, exist_ok=True)

    def print_help(self):
        """Print help information about script usage and processing stages"""
        help_text = """
    FranceDEM Processor - Processes French DEM data into high and low resolution tiles

    USAGE:
        python process_france_dem.py --working-dir DIR --existing-tileset-dir DIR [OPTIONS]

    REQUIRED PARAMETERS:
        --working-dir DIR          Working directory for all output files and temporary data
        --existing-tileset-dir DIR  Directory containing existing lowres tileset (highres will be DIR_highres)

    OPTIONAL PARAMETERS:
        --max-jobs N               Number of parallel threads for processing (default: 18)
        --debug                    Enable debug mode with verbose output
        --skip-download, -s        Skip downloading source files
        --skip-empty-check, -e     Skip empty tiles check
        --skip-enhance             Skip tile enhancement stage
        --skip-clip-by-land        Skip land clipping stage (also skips land data download)
        --skip-coastline-mask      Skip coastline mask stage (also skips coastline data download)
        --skip-process-archives    Skip archive processing stage
        --skip-postprocessing      Skip postprocessing stage (NoData replacement and compression)

    PROCESSING STAGES:
        1. DOWNLOAD SOURCE FILES    [--skip-download]
            - Downloads DEM archives from IGN France
        2. PROCESS ARCHIVES         [--skip-process-archives]
            - Extracts and converts ASC files to GeoTIFF
            - Reprojects to EPSG:4326
        3. MERGE TIFF FILES
            - Combines all processed TIFF files into single mosaic
        4. CHECK EMPTY TILES        [--skip-empty-check]
            - Pre-checks which tiles contain valid elevation data to speedup
        5. CREATE TILES
            - Generates highres (16384px) and lowres (3600px) tiles
            5.1. ENHANCE TILES            [--skip-enhance]
                5.1.1 COASTLINE MASK        [--skip-coastline-mask]        
                    - Applies coastline buffer masks for water/land separation
                    - Uses tiles_ignore_coastline_mask.csv to skip specific tiles
                        It contains a list of tiles where masking is not required
                        (if the new data does not cover the entire landmass tile area
                        and old data needs to be overlaid). The mask is created around
                        the coastline as a buffer with a radius of 0.004 degrees.
                        During processing, this mask is filled with zeros and then
                        overlaid on the existing old tile.
                        This is necessary to avoid the problem of old data sometimes
                        appearing near the coastline when new data doesn't cover
                        the entire land area. This happens when the new data doesn't
                        include the sea.
                        Usually this file should contain edge tiles of land coastline.
                5.1.2 MERGING WITH EXISTING DATA
                    - Improves tiles using existing data
                    - Uses tiles_ignore_enhance.csv to skip specific tiles
            5.2. POSTPROCESSING           [--skip-postprocessing]
                5.2.1 CLIP BY LAND [--skip-clip-by-land]
                    - Clips tiles to land polygons
                    - Uses tiles_ignore_clip_by_land.csv to skip specific tiles
                5.2.2 HANDLE NODATA
                    - Replaces NoData values (-9999) with 0 and sets new NoData value to -9999
                    - Final compression and optimization of tiles
                
    CONFIGURATION FILES:
        tiles_enhance.csv          List of tiles to enhance (all tiles if file missing)
        tiles_ignore_enhance.csv   List of tiles to skip enhancement
        tiles_clip_by_land.csv     List of tiles to clip by land (all tiles if file missing)
        tiles_ignore_clip_by_land.csv List of tiles to skip land clipping
        tiles_ignore_coastline_mask.csv List of tiles to skip coastline masking

    EXAMPLES:
        python process_france_dem.py --working-dir /data/france_dem --existing-tileset-dir /data/srtm
        python process_france_dem.py --working-dir /data/france_dem --existing-tileset-dir /data/srtm --skip-download --skip-enhance --max-jobs 8
            """
        print(help_text)

    def load_empty_tiles_cache(self):
        """Load empty tiles cache from file"""
        if not os.path.exists(self.empty_tiles_cache_file):
            return set()

        try:
            with open(self.empty_tiles_cache_file, 'r') as f:
                cache_data = json.load(f)

            # Check cache creation time
            cache_time = datetime.fromisoformat(cache_data.get('timestamp', '2000-01-01'))
            expiry_time = cache_time + timedelta(hours=self.cache_expiry_hours)

            if datetime.now() > expiry_time:
                print("Empty tiles cache expired, creating new one")
                return set()

            empty_tiles = set(cache_data.get('empty_tiles', []))

            print(f"Loaded empty tiles cache: {len(empty_tiles)} records")
            return empty_tiles

        except Exception as e:
            print(f"Error loading empty tiles cache: {e}")
            return set()

    def save_empty_tiles_cache(self, empty_tiles):
        """Save empty tiles cache to file"""
        try:
            cache_data = {
                'timestamp': datetime.now().isoformat(),
                'empty_tiles': list(empty_tiles)
            }

            with open(self.empty_tiles_cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)

            print(f"Saved empty tiles cache: {len(empty_tiles)} records")

        except Exception as e:
            print(f"Error saving empty tiles cache: {e}")

    def get_tile_bounds(self, input_tiff):
        """Get tile bounds for tile creation"""
        try:
            result = subprocess.run([
                'gdalinfo', input_tiff
            ], capture_output=True, text=True, check=True)

            # Parse bounds
            bounds = {}
            for line in result.stdout.split('\n'):
                if 'Upper Left' in line:
                    coords = re.findall(r'\(([^)]+)\)', line)
                    if coords:
                        x, y = map(float, coords[0].split(','))
                        bounds['ul_x'] = x
                        bounds['ul_y'] = y
                elif 'Lower Right' in line:
                    coords = re.findall(r'\(([^)]+)\)', line)
                    if coords:
                        x, y = map(float, coords[0].split(','))
                        bounds['lr_x'] = x
                        bounds['lr_y'] = y

            # Determine tile bounds
            min_lon = int(np.floor(bounds['ul_x']))
            max_lon = int(np.ceil(bounds['lr_x']))
            min_lat = int(np.floor(bounds['lr_y']))
            max_lat = int(np.ceil(bounds['ul_y']))

            print(f"Region bounds: longitude {min_lon}°-{max_lon}°, latitude {min_lat}°-{max_lat}°")
            print(f"Total tiles to process: {(max_lon - min_lon) * (max_lat - min_lat)}")

            return min_lon, max_lon, min_lat, max_lat

        except Exception as e:
            print(f"Error getting tile bounds: {e}")
            return 0, 0, 0, 0

    def find_existing_tile(self, tile_name):
        """Find existing tile in old datasets"""
        # First check highres
        if self.existing_tileset_highres_dir:
            highres_path = os.path.join(self.existing_tileset_highres_dir, tile_name)
            if os.path.exists(highres_path):
                return highres_path

        # Then check lowres
        if self.existing_tileset_dir:
            lowres_path = os.path.join(self.existing_tileset_dir, tile_name)
            if os.path.exists(lowres_path):
                return lowres_path

        return None

    def crop_tile_to_boundaries(self, tile_path, tile_name, tile_size=None):
        """Crop tile to 1x1 degree grid according to its name with 1-pixel extension"""
        try:
            # Parse coordinates from filename
            match = re.match(r'([NS])(\d{2})([EW])(\d{3})\.tif', tile_name)
            if not match:
                print(f"Invalid file name format: {tile_name}")
                return tile_path

            ns, lat_str, ew, lon_str = match.groups()
            lat = int(lat_str)
            lon = int(lon_str)

            # Adjust coordinates based on hemisphere
            if ns == 'S':
                lat = -lat
            if ew == 'W':
                lon = -lon

            # Get tile dimensions if not provided
            if tile_size is None:
                tile_info = subprocess.run([
                    'gdalinfo', tile_path
                ], capture_output=True, text=True, check=True)

                for line in tile_info.stdout.split('\n'):
                    if 'Size is' in line:
                        dimensions = re.findall(r'Size is (\d+), (\d+)', line)
                        if dimensions:
                            tile_size = int(dimensions[0][0])
                            break

            # Calculate 1-pixel extension in degrees
            # Assuming tile covers exactly 1x1 degree
            pixel_size_x = 1.0 / tile_size
            pixel_size_y = 1.0 / tile_size

            # Extend by 1 pixel on each side
            extension_x = pixel_size_x
            extension_y = pixel_size_y

            xmin = lon - extension_x
            xmax = lon + 1 + extension_x
            ymin = lat - extension_y
            ymax = lat + 1 + extension_y

            # Create temporary cropped file
            cropped_tile = os.path.join(self.temp_dir, f"cropped_{tile_name}")

            # Crop tile to extended degree grid
            subprocess.run([
                'gdalwarp',
                '-te', str(xmin), str(ymin), str(xmax), str(ymax),
                '-co', 'COMPRESS=LZW',
                '-co', 'PREDICTOR=2',
                '-co', 'ZLEVEL=6',
                '-ot', 'Int16',
                '-overwrite',
                '-dstnodata', '-9999',
                tile_path, cropped_tile
            ], check=True, capture_output=True)

            if self.debug_mode:
                print(f"Cropped tile {tile_name} with 1-pixel extension: "
                      f"({lon},{lat})-({lon + 1},{lat + 1}) -> "
                      f"({xmin:.6f},{ymin:.6f})-({xmax:.6f},{ymax:.6f})")

            return cropped_tile

        except Exception as e:
            print(f"Error cropping tile {tile_name}: {e}")
            return tile_path

    def merge_tiles(self, new_tile_path, existing_tile_path, output_path, tile_size=None, tile_name=None):
        """Merge new and old tiles preserving resolution"""
        temp_vrt1 = None
        temp_vrt2 = None
        merged_vrt = None
        cropped_existing_tile = None
        upscaled_existing_tile = None

        try:
            # Create output directory if it doesn't exist
            output_dir = os.path.dirname(output_path)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)

            # If tile_name not provided, get from output_path
            if tile_name is None:
                tile_name = os.path.basename(output_path)

            # Check if we need to upscale the existing tile
            need_upscale = False
            if tile_size is not None:
                # Get existing tile dimensions
                existing_info = subprocess.run([
                    'gdalinfo', existing_tile_path
                ], capture_output=True, text=True, check=True)

                existing_width = None
                existing_height = None
                for line in existing_info.stdout.split('\n'):
                    if 'Size is' in line:
                        dimensions = re.findall(r'Size is (\d+), (\d+)', line)
                        if dimensions:
                            existing_width = int(dimensions[0][0])
                            existing_height = int(dimensions[0][1])
                            break

                # Check if existing tile resolution is more than 2 times smaller
                if existing_width and existing_height and tile_size > existing_width * 2:
                    need_upscale = True

            # Upscale existing tile if needed
            if need_upscale and tile_size is not None:
                upscaled_existing_tile = os.path.join(self.temp_dir, f"upscaled_{os.path.basename(existing_tile_path)}")

                subprocess.run([
                    'gdalwarp',
                    '-ts', str(tile_size), str(tile_size),
                    '-r', 'bilinear',
                    '-co', 'COMPRESS=LZW',
                    '-co', 'PREDICTOR=2',
                    '-co', 'ZLEVEL=6',
                    '-ot', 'Int16',
                    '-overwrite',
                    '-dstnodata', '-9999',
                    existing_tile_path, upscaled_existing_tile
                ], check=True, capture_output=True)

                # Use upscaled tile for further processing
                existing_tile_path = upscaled_existing_tile

            # Crop existing tile to degree grid with 1-pixel extension
            cropped_existing_tile = self.crop_tile_to_boundaries(existing_tile_path, tile_name, tile_size)

            # Create temporary VRT for each source
            temp_vrt1 = os.path.join(self.temp_dir, f"source1_{os.path.basename(new_tile_path)}.vrt")
            temp_vrt2 = os.path.join(self.temp_dir, f"source2_{os.path.basename(new_tile_path)}.vrt")
            merged_vrt = os.path.join(self.temp_dir, f"merged_{os.path.basename(new_tile_path)}.vrt")

            # Use absolute paths
            new_tile_abs = os.path.abspath(new_tile_path)
            existing_tile_abs = os.path.abspath(cropped_existing_tile)

            # Create VRT for existing tile (background) with absolute paths
            subprocess.run([
                'gdalbuildvrt',
                temp_vrt1,
                existing_tile_abs
            ], check=True, capture_output=True)

            # Create VRT for new tile (foreground) with absolute paths
            subprocess.run([
                'gdalbuildvrt',
                temp_vrt2,
                new_tile_abs
            ], check=True, capture_output=True)

            # Merge VRT (old first, then new) with absolute paths
            subprocess.run([
                'gdalbuildvrt',
                '-hidenodata',
                '-overwrite',
                merged_vrt,
                temp_vrt1,
                temp_vrt2
            ], check=True, capture_output=True)

            # Convert merged VRT to TIFF preserving resolution
            translate_cmd = [
                'gdal_translate',
                '-co', 'COMPRESS=LZW',
                '-co', 'PREDICTOR=2',
                '-co', 'ZLEVEL=6',
                '-ot', 'Int16',
                '-r', 'bilinear',
                '-a_nodata', '-9999',
            ]

            # Add tile size parameter if specified
            if tile_size is not None:
                translate_cmd.extend(['-outsize', str(tile_size), str(tile_size)])

            translate_cmd.extend([merged_vrt, output_path])

            subprocess.run(translate_cmd, check=True, capture_output=True)

            return True

        except Exception as e:
            print(f"Error merging tiles {os.path.basename(new_tile_path)}: {e}")
            return False
        finally:
            # Clean up temporary files
            self.cleanup_temp_files([temp_vrt1, temp_vrt2, merged_vrt, upscaled_existing_tile])
            # Clean up temporary cropped file
            if cropped_existing_tile and cropped_existing_tile != existing_tile_path:
                self.cleanup_temp_files([cropped_existing_tile])

    def _check_tile_has_data(self, input_tiff, lon, lat, tile_name):
        """Simplified check if tile area contains meaningful data using only STATISTICS_MAXIMUM with 1-pixel extension"""
        # Calculate approximate tile size for extension calculation
        # Use standard tile sizes: 16384 for highres, 3600 for lowres, or fallback to 2000 for check
        if 'highres' in self.highres_dir and os.path.basename(self.highres_dir) in tile_name:
            tile_size = self.highres_tile_size
        elif 'lowres' in self.lowres_dir and os.path.basename(self.lowres_dir) in tile_name:
            tile_size = self.lowres_tile_size
        else:
            tile_size = 2000  # Fallback for general check

        # Calculate 1-pixel extension in degrees
        pixel_size_x = 1.0 / tile_size
        pixel_size_y = 1.0 / tile_size

        # Extend by 1 pixel on each side
        extension_x = pixel_size_x
        extension_y = pixel_size_y

        # Tile area with 1-pixel extension
        xmin = lon - extension_x
        xmax = lon + 1 + extension_x
        ymin = lat - extension_y
        ymax = lat + 1 + extension_y

        try:
            # Create temporary file for detailed statistics check
            temp_check_file = os.path.join(self.temp_dir, f"check_{tile_name}")

            # Use larger size for more accurate sampling (with extended bounds)
            warp_cmd = [
                'gdalwarp',
                '-te', str(xmin), str(ymin), str(xmax), str(ymax),
                '-ts', '2000', '2000',  # Increased size for better sampling
                '-r', 'cubicspline',
                '-ot', 'Int16',
                '-dstnodata', '-9999',
                '-overwrite',
                input_tiff, temp_check_file
            ]

            subprocess.run(warp_cmd, check=True, capture_output=True)

            # Get detailed statistics using gdalinfo
            stats_result = subprocess.run([
                'gdalinfo', '-stats', temp_check_file
            ], capture_output=True, text=True, check=True)

            # Parse only maximum value
            max_value = None
            for line in stats_result.stdout.split('\n'):
                if 'STATISTICS_MAXIMUM=' in line:
                    max_value = float(line.split('=')[1])
                    break

            # Simple validation: tile has data if maximum value is greater than -9998
            # (allowing for small rounding errors near NoData value)
            has_data = max_value is not None and max_value > 0

            if self.debug_mode:
                print(f"Tile {tile_name}: STATISTICS_MAXIMUM={max_value} -> {has_data}")

            # Delete temporary check file only in non-debug mode
            if not self.debug_mode:
                self.cleanup_temp_files([temp_check_file])

            return has_data

        except subprocess.CalledProcessError as e:
            # If statistics check fails, consider tile empty
            if self.debug_mode:
                print(f"Error checking tile {tile_name}: {e}")
                print(f"Debug mode: temporary check file preserved: {temp_check_file}")
            else:
                self.cleanup_temp_files([temp_check_file])
            return False

    def precheck_empty_tiles(self, input_tiff, min_lon, max_lon, min_lat, max_lat):
        """Multi-threaded pre-check of empty tiles using cache"""
        # Load empty tiles cache
        cached_empty_tiles = self.load_empty_tiles_cache()

        valid_tiles = set()
        empty_tiles = set(cached_empty_tiles)  # Start with cached empty tiles
        lock = threading.Lock()

        total_tiles = (max_lon - min_lon) * (max_lat - max_lat)

        # Determine tiles to check (excluding already cached empty ones)
        tiles_to_check = []
        for lon in range(min_lon, max_lon):
            for lat in range(min_lat, max_lat):
                # Convert coordinates to tile name for cache comparison
                ns = 'N' if lat >= 0 else 'S'
                ew = 'E' if lon >= 0 else 'W'
                tile_name = f"{ns}{abs(lat):02d}{ew}{abs(lon):03d}"

                if tile_name not in cached_empty_tiles:
                    tiles_to_check.append((lon, lat))
                else:
                    # Add cached empty tiles to result
                    empty_tiles.add(tile_name)

        print(f"Performing multi-threaded data presence check in tiles...")

        # If all tiles already in cache, return result
        if not tiles_to_check:
            # Determine valid tiles (all others except empty)
            for lon in range(min_lon, max_lon):
                for lat in range(min_lat, max_lat):
                    ns = 'N' if lat >= 0 else 'S'
                    ew = 'E' if lon >= 0 else 'W'
                    tile_name = f"{ns}{abs(lat):02d}{ew}{abs(lon):03d}"
                    if tile_name not in empty_tiles:
                        valid_tiles.add((lon, lat))

            print(f"Pre-check completed (cache used): {len(valid_tiles)} valid, {len(empty_tiles)} empty tiles")
            return valid_tiles, empty_tiles

        # Counter for progress tracking
        processed_count = 0
        progress_lock = threading.Lock()

        # Determine step for progress updates (every 1% or minimum 10 tiles)
        progress_step = max(10, len(tiles_to_check) // 10)
        last_reported = 0

        def check_worker(tile_queue):
            nonlocal processed_count, last_reported
            while True:
                tile_info = tile_queue.get()
                if tile_info is None:
                    break

                lon, lat = tile_info
                ns = 'N' if lat >= 0 else 'S'
                ew = 'E' if lon >= 0 else 'W'
                tile_name_without_ext = f"{ns}{abs(lat):02d}{ew}{abs(lon):03d}"
                tile_name = f"{tile_name_without_ext}.tif"

                has_data = self._check_tile_has_data(input_tiff, lon, lat, tile_name)

                with lock:
                    if has_data:
                        valid_tiles.add((lon, lat))
                    else:
                        empty_tiles.add(tile_name_without_ext)

                # Update progress
                with progress_lock:
                    processed_count += 1
                    if processed_count - last_reported >= progress_step:
                        percentage = (processed_count / len(tiles_to_check)) * 100
                        print(f"{processed_count}/{len(tiles_to_check)} ({percentage:.1f}%)")
                        last_reported = processed_count

                tile_queue.task_done()

        # Create queue
        queue = Queue()
        threads = []

        # Start workers
        num_workers = min(self.max_jobs, len(tiles_to_check))
        for _ in range(num_workers):
            thread = threading.Thread(target=check_worker, args=(queue,))
            thread.start()
            threads.append(thread)

        # Add only tiles for checking to queue
        for tile_info in tiles_to_check:
            queue.put(tile_info)

        # Wait for processing completion
        queue.join()

        # Print final progress
        print(f"Check progress: {processed_count}/{len(tiles_to_check)} (100.0%)")

        # Stop workers
        for _ in range(num_workers):
            queue.put(None)

        for thread in threads:
            thread.join()

        # Save updated empty tiles cache
        self.save_empty_tiles_cache(empty_tiles)

        print(f"Pre-check completed: {len(valid_tiles)} valid, {len(empty_tiles)} empty tiles")
        return valid_tiles, empty_tiles

    def _create_single_tile(self, input_tiff, output_dir, tile_size, lon, lat, skip_data_check=False):
        """Create single tile with data presence check and 1-pixel extension"""
        # Form filename
        ns = 'N' if lat >= 0 else 'S'
        ew = 'E' if lon >= 0 else 'W'
        tile_name = f"{ns}{abs(lat):02d}{ew}{abs(lon):03d}.tif"
        output_path = os.path.join(output_dir, tile_name)

        # Skip if file already exists
        if os.path.exists(output_path):
            return True

        # Calculate 1-pixel extension in degrees
        pixel_size_x = 1.0 / tile_size
        pixel_size_y = 1.0 / tile_size

        # Extend by 1 pixel on each side
        extension_x = pixel_size_x
        extension_y = pixel_size_y

        # Tile area with 1-pixel extension
        xmin = lon - extension_x
        xmax = lon + 1 + extension_x
        ymin = lat - extension_y
        ymax = lat + 1 + extension_y

        # Skip data check if using pre-check
        # Tiles already checked in precheck_empty_tiles, so skip_data_check=True
        if not skip_data_check:
            # Check data presence in tile area (using extended bounds)
            if not self._check_tile_has_data(input_tiff, lon, lat, tile_name):
                if self.debug_mode:
                    print(f"Skipping empty tile: {tile_name}")
                return False

        try:
            # Create tile with source data (without replacing NoData and negative values)
            warp_cmd = [
                'gdalwarp',
                '-te', str(xmin), str(ymin), str(xmax), str(ymax),
                '-te_srs', 'EPSG:4326',
                '-ts', str(tile_size), str(tile_size),
                '-r', 'cubicspline',
                '-ot', 'Int16',
                '-dstnodata', '-9999',  # Keep original NoData value
                '-co', 'COMPRESS=LZW',
                '-co', 'PREDICTOR=2',
                '-co', 'ZLEVEL=6',
                '-co', 'NUM_THREADS=7',
            ]

            # Add input and output files
            warp_cmd.extend([input_tiff, output_path])

            subprocess.run(warp_cmd, check=True, capture_output=True)

            # In debug mode save original created tile with suffix
            if self.debug_mode:
                base_name = os.path.splitext(tile_name)[0]
                raw_tile_path = os.path.join(output_dir, f"{base_name}_raw5.tif")
                shutil.copy2(output_path, raw_tile_path)
                print(f"Saved raw tile: {raw_tile_path}")
                print(f"Created tile with 1-pixel extension: {tile_name} "
                      f"bounds: ({xmin:.6f},{ymin:.6f})-({xmax:.6f},{ymax:.6f})")

            print(f"Created tile: {tile_name}")
            return True

        except subprocess.CalledProcessError as e:
            # Skip tiles with creation errors
            if os.path.exists(output_path):
                os.remove(output_path)
            print(f"Error creating tile: {tile_name} - {e}")
            # Print full error log
            print(f"Full error output: {e.stderr if hasattr(e, 'stderr') else 'No additional information'}")
            return False

    def download_coastline(self):
        """Download coastline from OSMData"""
        coastline_zip = os.path.join(self.working_dir, "coastlines-split-4326.zip")
        coastline_shp = os.path.join(self.working_dir, "coastline.shp")

        if os.path.exists(coastline_shp):
            print(f"Coastline already downloaded: {coastline_shp}")
            return coastline_shp

        print("Downloading coastline from OSMData...")

        # URL for coastline download
        coastline_url = "https://osmdata.openstreetmap.de/download/coastlines-split-4326.zip"

        try:
            # Download archive
            subprocess.run(['wget', '-q', '-O', coastline_zip, coastline_url], check=True)

            # Extract archive
            subprocess.run(['unzip', '-q', '-o', coastline_zip], check=True)

            # Find main shapefile in extracted structure
            coastline_shp_path = None
            for root, dirs, files in os.walk(os.path.join(self.working_dir, 'coastlines-split-4326')):
                for file in files:
                    if file.endswith('.shp') and 'lines' in file:
                        coastline_shp_path = os.path.join(root, file)
                        break
                if coastline_shp_path:
                    break

            if not coastline_shp_path:
                print("Could not find shapefile in coastline archive")
                return None

            # Copy to working directory for convenience
            shutil.copy2(coastline_shp_path, coastline_shp)

            # Copy related files
            base_name = os.path.splitext(coastline_shp_path)[0]
            for ext in ['.shx', '.dbf', '.prj', '.cpg']:
                src_file = base_name + ext
                if os.path.exists(src_file):
                    dst_file = os.path.splitext(coastline_shp)[0] + ext
                    shutil.copy2(src_file, dst_file)

            print(f"Coastline saved: {coastline_shp}")
            return coastline_shp

        except subprocess.CalledProcessError as e:
            print(f"Error downloading coastline: {e}")
            return None
        except Exception as e:
            print(f"Error processing coastline: {e}")
            return None

    def create_coastline_buffer_mask(self, lon, lat, tile_size=None):
        """Create buffer mask for coastline using OGR - covering entire tile with 1-pixel extension"""
        # Calculate 1-pixel extension in degrees
        if tile_size is None:
            # Use highres tile size as default for coastline mask
            tile_size = self.highres_tile_size

        pixel_size_x = 1.0 / tile_size
        pixel_size_y = 1.0 / tile_size

        # Extend by 1 pixel on each side
        extension_x = pixel_size_x
        extension_y = pixel_size_y

        # Determine tile bounds with 1-pixel extension
        xmin = lon - extension_x
        xmax = lon + 1 + extension_x
        ymin = lat - extension_y
        ymax = lat + 1 + extension_y

        # Create temporary files
        clipped_coastline = os.path.join(self.coastline_temp_dir, f"coastline_{lon}_{lat}.shp")
        buffered_coastline = os.path.join(self.coastline_temp_dir, f"coastline_buffer_{lon}_{lat}.shp")
        tile_polygon = os.path.join(self.coastline_temp_dir, f"tile_polygon_{lon}_{lat}.shp")

        try:
            # 1. Extract coastline section corresponding to extended tile size
            subprocess.run([
                'ogr2ogr',
                '-f', 'ESRI Shapefile',
                '-overwrite',
                '-spat', str(xmin), str(ymin), str(xmax), str(ymax),
                '-clipsrc', str(xmin), str(ymin), str(xmax), str(ymax),
                clipped_coastline,
                self.coastline_path
            ], check=True)

            # Check that file exists and is not empty
            if not os.path.exists(clipped_coastline):
                if self.debug_mode:
                    print(f"No coastline data for tile {lon},{lat}")
                return None

            # Check feature count
            info_result = subprocess.run([
                'ogrinfo', '-ro', '-so', '-q', clipped_coastline
            ], capture_output=True, text=True)

            if info_result.returncode != 0 or 'Feature Count: 0' in info_result.stdout:
                if self.debug_mode:
                    print(f"Empty coastline for tile {lon},{lat}")
                return None

            # 2. Create tile boundary polygon
            subprocess.run([
                'ogr2ogr',
                '-f', 'ESRI Shapefile',
                '-overwrite',
                '-dialect', 'sqlite',
                '-sql',
                f"SELECT ST_GeomFromText('POLYGON(({xmin} {ymin}, {xmax} {ymin}, {xmax} {ymax}, {xmin} {ymax}, {xmin} {ymin}))') as geometry",
                tile_polygon,
                clipped_coastline
            ], check=True, capture_output=True)

            # 3. Create buffer around coastline and merge with tile boundaries
            buffer_cmd = [
                'ogr2ogr',
                '-f', 'ESRI Shapefile',
                '-overwrite',
                '-dialect', 'sqlite',
                '-sql',
                f"SELECT ST_Union(ST_Buffer(geometry, 0.008)) as geometry FROM '{os.path.splitext(os.path.basename(clipped_coastline))[0]}'",
                buffered_coastline,
                clipped_coastline
            ]

            subprocess.run(buffer_cmd, check=True, capture_output=True)

            # Check that buffer file was created
            if not os.path.exists(buffered_coastline):
                if self.debug_mode:
                    print(f"Buffer file not created for tile {lon},{lat}")
                return None

            # Save original buffered coastline in debug mode
            if self.debug_mode:
                original_buffer = os.path.join(self.coastline_temp_dir,
                                                   f"coastline_buffer_original_{lon}_{lat}.shp")
                for ext in ['.shp', '.shx', '.dbf', '.prj', '.cpg']:
                    src = buffered_coastline.replace('.shp', ext)
                    dst = original_buffer.replace('.shp', ext)
                    if os.path.exists(src):
                        shutil.copy2(src, dst)
                print(f"Saved original buffer mask: {original_buffer}")

            # 4. Create inverted mask: entire tile MINUS buffered coastline area
            inverted_buffer = os.path.join(self.coastline_temp_dir, f"coastline_buffer_inverted_{lon}_{lat}.shp")

            invert_cmd = [
                'ogr2ogr',
                '-f', 'ESRI Shapefile',
                '-overwrite',
                '-dialect', 'sqlite',
                '-sql',
                f"SELECT ST_Difference(ST_GeomFromText('POLYGON(({xmin} {ymin}, {xmax} {ymin}, {xmax} {ymax}, {xmin} {ymax}, {xmin} {ymin}))'), ST_Union(geometry)) as geometry FROM '{os.path.splitext(os.path.basename(buffered_coastline))[0]}'",
                inverted_buffer,
                buffered_coastline
            ]

            subprocess.run(invert_cmd, check=True, capture_output=True)

            # Replace original buffer file with inverted one
            for ext in ['.shp', '.shx', '.dbf', '.prj']:
                old_file = buffered_coastline.replace('.shp', ext)
                new_file = inverted_buffer.replace('.shp', ext)
                if os.path.exists(old_file):
                    os.remove(old_file)
                if os.path.exists(new_file):
                    shutil.move(new_file, old_file)

            # Clean up temporary files
            self.cleanup_temp_files([tile_polygon])

            # Debug information
            if self.debug_mode:
                buffer_info_path = os.path.join(self.coastline_temp_dir, f"buffer_info_{lon}_{lat}.txt")
                with open(buffer_info_path, 'w') as f:
                    f.write(f"INVERTED buffer mask for tile: {lon},{lat}\n")
                    f.write(f"Tile bounds: {xmin}, {ymin}, {xmax}, {ymax}\n")
                    f.write(f"Buffer distance: 0.004 degrees\n")
                    f.write(f"Method: Entire tile MINUS buffered coastline\n")

                print(f"Created INVERTED buffer mask covering entire tile {lon},{lat}")

            return buffered_coastline

        except subprocess.CalledProcessError as e:
            if self.debug_mode:
                print(f"Error creating buffer mask for tile {lon},{lat}: {e}")
                if hasattr(e, 'stderr') and e.stderr:
                    print(f"Stderr: {e.stderr}")
            return None
        except Exception as e:
            if self.debug_mode:
                print(f"Error creating buffer mask for tile {lon},{lat}: {e}")
            return None

    def apply_coastline_mask_to_tile(self, tile_path, coastline_mask_path):
        """Apply coastline mask to tile - cut out buffer area"""
        if not coastline_mask_path or not os.path.exists(coastline_mask_path):
            return False

        try:
            # Create temporary file
            temp_tile = os.path.join(self.temp_dir, f"coastline_masked_{os.path.basename(tile_path)}")

            # INVERT MASK: cut area by coastline mask
            # Now keep only area OUTSIDE buffer (land), and area inside buffer (water) becomes NoData
            subprocess.run([
                'gdalwarp',
                '-cutline', coastline_mask_path,
                '-cwhere', '1=1',  # Apply to all features
                '-cblend', '0',  # Disable edge blending
                '-co', 'COMPRESS=LZW',
                '-co', 'PREDICTOR=2',
                '-co', 'ZLEVEL=6',
                '-overwrite',
                '-dstnodata', '-9999',
                tile_path, temp_tile
            ], check=True, capture_output=True)

            # Replace original file
            shutil.copy2(temp_tile, tile_path)

            # Delete temporary file
            if not self.debug_mode:
                self.cleanup_temp_files([temp_tile])

            if self.debug_mode:
                print(f"Cut buffer area from tile: {os.path.basename(tile_path)}")

            return True

        except subprocess.CalledProcessError as e:
            if self.debug_mode:
                print(f"Error applying coastline mask: {e}")
            return False
        except Exception as e:
            if self.debug_mode:
                print(f"Error applying coastline mask: {e}")
            return False

    def _load_tile_list_from_csv(self, csv_file):
        """Universal function to load tile list from CSV file"""
        if not os.path.exists(csv_file):
            print(f"File not found: {csv_file}")
            return set()

        try:
            tiles = set()
            with open(csv_file, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue

                    # Skip first line if it's a header
                    if line_num == 1 and ('id' in line.lower() or 'lon' in line.lower() or 'lat' in line.lower()):
                        continue

                    # Split line by commas
                    parts = line.split(',')
                    if len(parts) >= 7:
                        # Extract tile name from last column
                        tile_name = parts[-1].strip()
                        if tile_name:
                            tiles.add(tile_name)
                            if self.debug_mode:
                                print(f"Added tile from {csv_file}: {tile_name}")
                    else:
                        # If format not CSV, try to extract tile name directly
                        # Check if line matches tile name format (N23E002)
                        if re.match(r'^[NS]\d{2}[EW]\d{3}$', line):
                            tiles.add(line)
                            if self.debug_mode:
                                print(f"Added tile from {csv_file}: {line}")
                        else:
                            print(f"Skipped line {line_num} with invalid format: {line}")

            print(f"Loaded tile list from {csv_file}: {len(tiles)} tiles")
            if self.debug_mode and tiles:
                print(f"Examples of loaded tiles from {csv_file}:", list(tiles)[:5])
            return tiles

        except Exception as e:
            print(f"Error loading tile list from {csv_file}: {e}")
            return set()

    def load_tiles_ignore_coastline_mask(self):
        """Load list of tiles to ignore coastline mask from file"""
        return self._load_tile_list_from_csv(self.tiles_ignore_coastline_mask_file)

    def load_tiles_to_enhance(self):
        """Load list of tiles to enhance from file"""
        if not os.path.exists(self.tiles_to_enhance_file):
            print(f"Tile enhancement list file not found: {self.tiles_to_enhance_file}")
            return None  # Return None instead of empty set

        return self._load_tile_list_from_csv(self.tiles_to_enhance_file)

    def load_tiles_to_clip_by_land(self):
        """Load list of tiles to clip by land from file"""
        if not os.path.exists(self.tiles_to_clip_by_land_file):
            print(f"Tile land clipping list file not found: {self.tiles_to_clip_by_land_file}")
            return None  # Return None instead of empty set

        return self._load_tile_list_from_csv(self.tiles_to_clip_by_land_file)

    def load_tiles_ignore_clip_by_land(self):
        """Load list of tiles to ignore land clipping from file"""
        return self._load_tile_list_from_csv(self.tiles_ignore_clip_by_land_file)

    def load_tiles_ignore_enhance(self):
        """Load list of tiles to ignore enhancement from file"""
        return self._load_tile_list_from_csv(self.tiles_ignore_enhance_file)

    def _enhance_single_tile(self, input_dir, tile_name, tile_size):
        """Enhance single tile with coastline mask application"""
        # Check if this tile should be enhanced
        tile_base_name = os.path.splitext(tile_name)[0]

        # Check if tile is in ignore list
        if tile_base_name in self.tiles_ignore_enhance:
            if self.debug_mode:
                print(f"Skipping enhancement for tile: {tile_name}")
            return False

        # If enhancement list not loaded (None), enhance ALL tiles
        # If list empty (set()), do NOT enhance any tiles
        # If list contains tiles, enhance only those in the list
        if self.tiles_to_enhance is not None and tile_base_name not in self.tiles_to_enhance:
            return False

        input_tile_path = os.path.join(input_dir, tile_name)

        # Check if old tile exists for this region
        existing_tile_path = self.find_existing_tile(tile_name)

        if not existing_tile_path:
            print(f"Old tile not found for enhancement: {tile_name}")
            return False

        # Check that source tile exists
        if not os.path.exists(input_tile_path):
            print(f"Warning: source tile not found {input_tile_path}")
            return False

        # Check that old tile exists
        if not os.path.exists(existing_tile_path):
            print(f"Warning: old tile not found {existing_tile_path}")
            return False

        # Parse coordinates from filename for coastline mask creation
        match = re.match(r'([NS])(\d{2})([EW])(\d{3})\.tif', tile_name)
        if not match:
            print(f"Invalid file name format: {tile_name}")
            return False

        ns, lat_str, ew, lon_str = match.groups()
        lat = int(lat_str)
        lon = int(lon_str)

        # Adjust coordinates based on hemisphere
        if ns == 'S':
            lat = -lat
        if ew == 'W':
            lon = -lon

        # In debug mode save old tile to output directory
        if self.debug_mode:
            base_name = os.path.splitext(tile_name)[0]
            old_tile_debug_path = os.path.join(input_dir, f"{base_name}_old_enhance512.tif")
            shutil.copy2(existing_tile_path, old_tile_debug_path)
            print(f"Saved old tile for debugging: {old_tile_debug_path}")

        # 1. Create coastline mask with buffer
        coastline_mask_path = None
        if self.coastline_path:
            # Check if mask should be ignored for this tile
            if tile_base_name in self.tiles_ignore_coastline_mask:
                if self.debug_mode:
                    print(f"Ignoring coastline mask for tile: {tile_name}")
            else:
                coastline_mask_path = self.create_coastline_buffer_mask(lon, lat)

        # 2. Apply mask to old tile - CUT OUT buffer area (make it NoData)
        if coastline_mask_path:
            # Create temporary copy of old tile
            temp_old_tile = os.path.join(self.temp_dir, f"old_masked_{tile_name}")
            shutil.copy2(existing_tile_path, temp_old_tile)

            # Apply mask to old tile - cut out buffer area
            self.apply_coastline_mask_to_tile(temp_old_tile, coastline_mask_path)
            existing_tile_path = temp_old_tile

            if self.debug_mode:
                print(f"Old tile prepared: buffer area cut out as NoData")
        else:
            if self.debug_mode and self.coastline_path and tile_base_name in self.tiles_ignore_coastline_mask:
                print(f"Coastline mask ignored for tile: {tile_name}")

        # 3. Merge tiles as usual
        temp_output = os.path.join(self.temp_dir, f"enhanced_{tile_name}")

        # Merge tiles (old under new) preserving resolution
        # Now in old tile buffer area is NoData, so new data (water) will be visible
        if self.merge_tiles(input_tile_path, existing_tile_path, temp_output, tile_size, tile_name):
            # In debug mode save intermediate result in final tiles directory
            if self.debug_mode:
                base_name = os.path.splitext(tile_name)[0]
                enhanced_debug_path = os.path.join(input_dir, f"{base_name}_enhanced512.tif")
                shutil.copy2(temp_output, enhanced_debug_path)
                print(f"Saved enhanced tile: {enhanced_debug_path}")

            # Replace source file with merged one
            shutil.copy2(temp_output, input_tile_path)

            # Delete temporary files
            self.cleanup_temp_files([temp_output])
            # Delete temporary copy of old tile if it was created
            if coastline_mask_path:
                self.cleanup_temp_files([os.path.join(self.temp_dir, f"old_masked_{tile_name}")])
            return True
        else:
            # If merge failed, delete temporary files
            self.cleanup_temp_files([temp_output])
            if coastline_mask_path:
                self.cleanup_temp_files([os.path.join(self.temp_dir, f"old_masked_{tile_name}")])
            return False

    def _postprocess_single_tile(self, input_dir, tile_name):
        """Postprocess single tile"""
        input_path = os.path.join(input_dir, tile_name)
        temp_tile = None

        try:
            # Parse coordinates from filename for land clipping
            match = re.match(r'([NS])(\d{2})([EW])(\d{3})\.tif', tile_name)
            if not match:
                print(f"Invalid file name format: {tile_name}")
                return False

            ns, lat_str, ew, lon_str = match.groups()
            lat = int(lat_str)
            lon = int(lon_str)

            # Adjust coordinates based on hemisphere
            if ns == 'S':
                lat = -lat
            if ew == 'W':
                lon = -lon

            # In debug mode create copies at each stage
            if self.debug_mode:
                base_name = os.path.splitext(tile_name)[0]

            # Step 1: Clip by land polygons (if tile in list or list missing)
            tile_base_name = os.path.splitext(tile_name)[0]

            # Check if land clipping should be ignored for this tile
            if tile_base_name in self.tiles_ignore_clip_by_land:
                if self.debug_mode:
                    print(f"Ignoring land clipping for tile: {tile_name}")
            else:
                # If land clipping list not loaded (None), clip ALL tiles
                # If list empty (set()), do NOT clip any tiles
                # If list contains tiles, clip only those in the list
                if self.land_polygons_path and (
                        self.tiles_to_clip_by_land is None or tile_base_name in self.tiles_to_clip_by_land):
                    land_clipped = self.clip_tile_to_land(input_path, lon, lat)
                    if not land_clipped:
                        print(f"Failed to clip tile by land: {tile_name}")

                    # In debug mode save land clipping result
                    if self.debug_mode and land_clipped:
                        land_clipped_debug_path = os.path.join(input_dir, f"{base_name}_land_clipped521.tif")
                        shutil.copy2(input_path, land_clipped_debug_path)
                        print(f"Saved land-clipped tile: {land_clipped_debug_path}")
                else:
                    if self.debug_mode:
                        if not self.land_polygons_path:
                            print(f"Skipping land clipping (data not loaded): {tile_name}")
                        elif self.tiles_to_clip_by_land is not None and tile_base_name not in self.tiles_to_clip_by_land:
                            print(f"Skipping land clipping (tile not in clipping list): {tile_name}")

            # Step 2: Replace NoData with 0 and set new NoData value
            temp_tile = os.path.join(self.temp_dir, tile_name)

            # In debug mode save NoData replacement result
            if self.debug_mode:
                nodata_replaced_path = os.path.join(input_dir, f"{base_name}_nodata_replaced521.tif")

            # Replace NoData -9999 with 0 and set new NoData value -9999
            subprocess.run([
                'gdalwarp',
                '-srcnodata', '-9999',
                '-dstnodata', '0',
                '-co', 'COMPRESS=LZW',
                '-co', 'PREDICTOR=2',
                '-co', 'ZLEVEL=6',
                input_path, temp_tile
            ], check=True, capture_output=True)

            # Set new NoData value to -9999
            subprocess.run([
                'gdal_edit.py',
                '-a_nodata', '-9999',
                temp_tile
            ], check=True, capture_output=True)

            # In debug mode save intermediate result
            if self.debug_mode:
                shutil.copy2(temp_tile, nodata_replaced_path)
                print(f"Saved nodata-replaced tile: {nodata_replaced_path}")

            # Replace source file with processed one
            shutil.copy2(temp_tile, input_path)

            # Delete temporary file
            self.cleanup_temp_files([temp_tile])

            print(f"Postprocessed tile: {tile_name}")
            return True

        except subprocess.CalledProcessError as e:
            print(f"Error postprocessing tile {tile_name}: {e}")
            self.cleanup_temp_files([temp_tile])
            return False

    def process_single_tile_worker(self, queue, input_tiff, output_dir, tile_size, process_type, skip_enhance=False,
                                   skip_clip_by_land=False, skip_coastline_mask=False, skip_postprocessing=False):
        """Worker for complete processing of single tile"""
        while True:
            tile_info = queue.get()
            if tile_info is None:
                break

            lon, lat = tile_info
            ns = 'N' if lat >= 0 else 'S'
            ew = 'E' if lon >= 0 else 'W'
            tile_name = f"{ns}{abs(lat):02d}{ew}{abs(lon):03d}.tif"
            tile_base_name = os.path.splitext(tile_name)[0]
            output_path = os.path.join(output_dir, tile_name)

            # UNIFIED CHECK: skip processing if final tile already exists
            if os.path.exists(output_path):
                if self.debug_mode:
                    print(f"Skipping tile processing (already exists): {tile_name}")
                queue.task_done()
                continue

            if process_type == "highres" or process_type == "lowres":
                # Step 1: Tile creation (skip data check, as already checked in precheck_empty_tiles)
                created = self._create_single_tile(input_tiff, output_dir, tile_size, lon, lat,
                                                   skip_data_check=True)  # Always skip check
                if not created:
                    queue.task_done()
                    continue

                # Step 2: Tile enhancement (always execute if list not empty or missing)
                # If tiles_to_enhance is None - enhance all tiles
                # If tiles_to_enhance is set() - do not enhance any tiles
                # If tiles_to_enhance contains tiles - enhance only them
                if not skip_enhance and (self.tiles_to_enhance is None or (
                        self.tiles_to_enhance and tile_base_name in self.tiles_to_enhance)):
                    enhanced = self._enhance_single_tile(output_dir, tile_name, tile_size)
                    if enhanced:
                        print(f"Enhanced tile: {tile_name}")
                    else:
                        print(f"Failed to enhance tile: {tile_name}")
                else:
                    if self.debug_mode:
                        print(f"Tile {tile_name} not in enhancement list")

                # Step 3: Postprocessing (unless skipped)
                if not skip_postprocessing:
                    self._postprocess_single_tile(output_dir, tile_name)
                else:
                    if self.debug_mode:
                        print(f"Skipping postprocessing for tile: {tile_name}")

            queue.task_done()

    def process_all_tiles(self, input_tiff, output_dir, tile_size=None, process_type="highres", prechecked_tiles=None,
                          skip_enhance=False, skip_clip_by_land=False, skip_coastline_mask=False,
                          skip_postprocessing=False):
        """Complete processing of all tiles: creation + enhancement + postprocessing"""
        if not os.path.exists(input_tiff):
            print(f"Source file not found: {input_tiff}")
            return

        os.makedirs(output_dir, exist_ok=True)

        # Get tile bounds for tile creation
        min_lon, max_lon, min_lat, max_lat = self.get_tile_bounds(input_tiff)

        if min_lon == max_lon or min_lat == max_lat:
            print("Failed to determine tile bounds")
            return

        total_tiles = (max_lon - min_lon) * (max_lat - max_lat)
        print(f"Processing tiles for region: {min_lon}°-{max_lon}° longitude, {min_lat}°-{max_lat}° latitude")
        print(f"Processing type: {process_type}, size: {tile_size}")

        # If pre-check not performed, execute it
        if prechecked_tiles is None:
            valid_tiles, empty_tiles = self.precheck_empty_tiles(input_tiff, min_lon, max_lon, min_lat, max_lat)
        else:
            valid_tiles, empty_tiles = prechecked_tiles

        print(f"Processing only {len(valid_tiles)} valid tiles (skipping {len(empty_tiles)} empty)")

        # Create task queue
        queue = Queue()
        threads = []

        # Start workers
        num_workers = min(self.max_jobs, len(valid_tiles))
        print(f"Starting {num_workers} threads for tile processing...")

        for _ in range(num_workers):
            thread = threading.Thread(target=self.process_single_tile_worker,
                                      args=(queue, input_tiff, output_dir, tile_size, process_type, skip_enhance,
                                            skip_clip_by_land, skip_coastline_mask, skip_postprocessing))
            thread.start()
            threads.append(thread)

        # Add only valid tiles to queue
        tiles_count = 0
        for lon, lat in valid_tiles:
            queue.put((lon, lat))
            tiles_count += 1

        print(f"Added to queue: {tiles_count} tiles")

        # Wait for processing completion
        queue.join()

        # Stop workers
        for _ in range(num_workers):
            queue.put(None)

        for thread in threads:
            thread.join()

        # print(f"Completed tile processing in {output_dir}")

    def cleanup_temp_files(self, file_paths):
        """Delete temporary file and all related files (.aux.xml etc.)"""
        if not file_paths:
            return

        for base_path in file_paths:
            if base_path is None:
                continue

            try:
                # Delete main file
                if os.path.exists(base_path):
                    os.remove(base_path)

                # Delete auxiliary XML files
                aux_xml_path = base_path + '.aux.xml'
                if os.path.exists(aux_xml_path):
                    os.remove(aux_xml_path)

                # Delete other possible auxiliary files
                for ext in ['.xml', '.ovr', '.msk', '.shp', '.shx', '.dbf', '.prj', '.cpg']:
                    aux_path = base_path + ext
                    if os.path.exists(aux_path):
                        os.remove(aux_path)

            except Exception as e:
                # Ignore temporary file deletion errors
                pass

    def cleanup_temp_directories(self):
        """Clean up temporary directories"""
        # In NON-debug mode clean up temporary coastline files
        if not self.debug_mode and os.path.exists(self.coastline_temp_dir):
            try:
                shutil.rmtree(self.coastline_temp_dir)
                os.makedirs(self.coastline_temp_dir, exist_ok=True)
            except Exception as e:
                print(f"Error cleaning coastline_temp_dir: {e}")
        else:
            print("Debug mode: temporary coastline files saved in", self.coastline_temp_dir)

        # Clean up other temporary directories if they exist and are empty
        temp_dirs = [self.temp_dir]
        for temp_dir in temp_dirs:
            if os.path.exists(temp_dir) and not os.listdir(temp_dir):
                try:
                    os.rmdir(temp_dir)
                except:
                    pass

    def get_file_links(self):
        """Get list of file links with _5M_"""
        print("Getting file list...")
        response = requests.get(self.url)
        soup = BeautifulSoup(response.content, 'html.parser')

        links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            if '.7z' in href and '_5M_' in href:
                if not href.startswith('http'):
                    if href.startswith('/'):
                        href = f"https://geoservices.ign.fr{href}"
                    else:
                        href = f"https://geoservices.ign.fr/{href}"
                links.append(href)

        # In debug mode take only first 5 files
        if self.debug_mode:
            links = links[:5]

        print(f"Found .7z files with _5M_: {len(links)}")
        return list(set(links))

    def download_file(self, url, queue):
        """Download file"""
        filename = os.path.basename(url)
        filepath = os.path.join(self.download_dir, filename)

        # Skip if file already exists
        if os.path.exists(filepath):
            queue.put(filepath)
            return

        print(f"Downloading: {filename}")
        try:
            subprocess.run(['wget', '-q', '-c', '-O', filepath, url], check=True)
            print(f"Success: {filename}")
            queue.put(filepath)
        except subprocess.CalledProcessError:
            print(f"Error: {filename}")

    def download_files_parallel(self, links):
        """Multi-threaded file downloading"""
        queue = Queue()
        threads = []

        for link in links:
            while threading.active_count() > self.max_jobs:
                threading.Event().wait(1)

            thread = threading.Thread(target=self.download_file, args=(link, queue))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        return [queue.get() for _ in links if not queue.empty()]

    def get_existing_archives(self):
        """Get list of already downloaded archives"""
        archives = []
        for file in os.listdir(self.download_dir):
            if '_5M_' in file and re.search(r'\.7z(\.\d{3})?$', file):
                archives.append(os.path.join(self.download_dir, file))

        # In debug mode take only first 5 archives
        if self.debug_mode:
            archives = archives[:5]

        print(f"Found already downloaded archives: {len(archives)}")
        return archives

    def extract_projection(self, archive_name):
        """Extract projection from archive name"""
        match = re.search(r'_ASC_([^_]+)_D', archive_name)
        if match:
            projection = match.group(1)
            projection_map = {
                "WGS84UTM20-MART87": "IGNF:RGAF09UTM20.MART87",
                "WGS84UTM20-GUAD88SM": "IGNF:RGAF09UTM20.GUAD88SM",
                "WGS84UTM20-GUAD88SB": "IGNF:RGAF09UTM20.GUAD88SB",
                "WGS84UTM20-GUAD88": "IGNF:RGAF09UTM20.GUAD88",
                "RGSPM06U21-STPM50": "IGNF:RGSPM06U21.STPM50",
                "RGR92UTM40S-REUN89": "IGNF:RGR92UTM40S.REUN89",
                "RGM04UTM38S-MAYO53": "IGNF:RGM04UTM38S.MAYO53",
                "LAMB93-IGN69": "IGNF:RGF93LAMB93.IGN69",
                "RGFG95UTM22-GUYA77": "IGNF:RGFG95UTM22.GUYA77",
                "LAMB93-IGN78C": "IGNF:RGF93LAMB93.IGN78C"
            }
            return projection_map.get(projection)
        return None

    def extract_asc_files(self, archive_path, temp_asc_dir):
        """Extract ASC files using 7z command"""
        try:
            # Extract all .asc files recursively
            result = subprocess.run(['7z', 'x', '-y', f'-o{temp_asc_dir}', archive_path, '-r', '*.asc'],
                                    capture_output=True, text=True, check=True)
            return True
        except subprocess.CalledProcessError as e:
            print(f"Error extracting {os.path.basename(archive_path)}: {e}")
            return False

    def is_already_processed(self, archive_name):
        """Check if archive already processed"""
        output_tiff = os.path.join(self.processed_source_dir, f"{archive_name}.tif")
        return os.path.exists(output_tiff)

    def process_archive(self, archive_path):
        """Process archive: extract, merge, convert"""
        archive_basename = os.path.basename(archive_path)
        match = re.match(r'(.*?)\.7z(?:\.(\d{3}))?$', archive_basename)
        if match:
            archive_name = match.group(1)
        else:
            archive_name = os.path.splitext(archive_basename)[0]

        # Skip if already processed
        if self.is_already_processed(archive_name):
            return

        temp_asc_dir = os.path.join(self.temp_dir, archive_name)
        os.makedirs(temp_asc_dir, exist_ok=True)

        print(f"Extracting: {archive_name}")

        # Extract ASC files
        if not self.extract_asc_files(archive_path, temp_asc_dir):
            print(f"Error extracting {archive_name}")
            return

        # Find ASC files
        asc_files = []
        for root, dirs, files in os.walk(temp_asc_dir):
            for file in files:
                if file.lower().endswith('.asc'):
                    asc_files.append(os.path.abspath(os.path.join(root, file)))  # Absolute paths

        if not asc_files:
            print(f"No ASC files found in {archive_name}")
            return

        # Create VRT
        output_vrt = os.path.join(self.temp_dir, f"{archive_name}.vrt")
        try:
            subprocess.run(['gdalbuildvrt', output_vrt] + asc_files, check=True)
        except subprocess.CalledProcessError:
            print(f"Error creating VRT for {archive_name}")
            return

        # Determine projection
        projection = self.extract_projection(archive_name)

        # Create TIFF in EPSG:4326
        output_tiff = os.path.join(self.processed_source_dir, f"{archive_name}.tif")

        if projection:
            # Create TIFF with correct projection and immediately reproject to EPSG:4326
            try:
                subprocess.run([
                    'gdalwarp',
                    '-s_srs', projection,
                    '-t_srs', 'EPSG:4326',
                    '-co', 'COMPRESS=LZW',
                    '-co', 'PREDICTOR=2',
                    '-co', 'ZLEVEL=9',
                    '-co', 'BIGTIFF=YES',
                    '-wo', 'NUM_THREADS=8',
                    '-co', 'TILED=YES',
                    '-multi',
                    '--config', 'GDAL_CACHEMAX', '512',
                    '-ot', 'Int16',
                    '-dstnodata', '-32767',
                    '-r', 'cubicspline',
                    output_vrt, output_tiff
                ], check=True, capture_output=True)
            except subprocess.CalledProcessError:
                print(f"Error reprojecting for {archive_name}")
        else:
            # If projection not defined, create TIFF without reprojection
            try:
                subprocess.run([
                    'gdal_translate',
                    '-co', 'COMPRESS=LZW',
                    '-co', 'PREDICTOR=2',
                    '-co', 'ZLEVEL=9',
                    '-co', 'BIGTIFF=YES',
                    '-co', 'NUM_THREADS=2',
                    output_vrt, output_tiff
                ], check=True, capture_output=True)
            except subprocess.CalledProcessError:
                print(f"Error converting for {archive_name}")

        # Clean up temporary files
        try:
            os.remove(output_vrt)
            shutil.rmtree(temp_asc_dir)
        except:
            pass

    def process_archive_worker(self, queue):
        """Worker for processing archives"""
        while True:
            archive_path = queue.get()
            if archive_path is None:
                break
            self.process_archive(archive_path)
            queue.task_done()

    def process_all_archives(self, archive_paths):
        """Process all archives in parallel"""
        # Group archives by base name
        groups = defaultdict(list)
        for path in archive_paths:
            basename = os.path.basename(path)
            match = re.match(r'(.*?)\.7z(?:\.(\d{3}))?$', basename)
            if match:
                base, part = match.groups()
                groups[base].append((part, path))

        main_archives = []
        for base, parts_list in groups.items():
            if not parts_list[0][0]:  # Single volume
                main_archives.append(parts_list[0][1])
            else:
                # Sort by part number
                parts_list.sort(key=lambda x: int(x[0]) if x[0] else 0)
                # Take first part (001)
                if parts_list[0][0] == '001':
                    main_archives.append(parts_list[0][1])
                else:
                    print(f"Missing first part for {base}")

        # In debug mode take only first 5 main archives
        if self.debug_mode:
            main_archives = main_archives[:5]

        print(f"Processing archives ({self.max_jobs} threads, found {len(main_archives)} main archives)...")

        queue = Queue()
        threads = []

        # Start workers
        for _ in range(self.max_jobs):
            thread = threading.Thread(target=self.process_archive_worker, args=(queue,))
            thread.start()
            threads.append(thread)

        # Add archives to queue
        for archive_path in main_archives:
            queue.put(archive_path)

        # Wait for processing completion
        queue.join()

        # Stop workers
        for _ in range(self.max_jobs):
            queue.put(None)

        for thread in threads:
            thread.join()

    def download_land_polygons(self):
        """Download land polygons from OSMData"""
        land_polygons_zip = os.path.join(self.working_dir, "land-polygons-complete-4326.zip")
        land_polygons_shp = os.path.join(self.working_dir, "land_polygons.shp")

        if os.path.exists(land_polygons_shp):
            print(f"Land polygons already downloaded: {land_polygons_shp}")
            return land_polygons_shp

        print("Downloading land polygons from OSMData...")

        # URL for land polygons download
        land_url = "https://osmdata.openstreetmap.de/download/land-polygons-complete-4326.zip"

        try:
            # Download archive
            subprocess.run(['wget', '-q', '-O', land_polygons_zip, land_url], check=True)

            # Extract archive
            subprocess.run(['unzip', '-q', '-o', land_polygons_zip], check=True)

            # Find main shapefile in extracted structure
            land_shp_path = None
            for root, dirs, files in os.walk(os.path.join(self.working_dir, 'land-polygons-complete-4326')):
                for file in files:
                    if file.endswith('.shp') and 'land_polygons' in file:
                        land_shp_path = os.path.join(root, file)
                        break
                if land_shp_path:
                    break

            if not land_shp_path:
                print("Could not find shapefile in land polygons archive")
                return None

            # Copy to working directory for convenience
            shutil.copy2(land_shp_path, land_polygons_shp)

            # Copy related files
            base_name = os.path.splitext(land_shp_path)[0]
            for ext in ['.shx', '.dbf', '.prj', '.cpg']:
                src_file = base_name + ext
                if os.path.exists(src_file):
                    dst_file = os.path.splitext(land_polygons_shp)[0] + ext
                    shutil.copy2(src_file, dst_file)

            print(f"Land polygons saved: {land_polygons_shp}")
            return land_polygons_shp

        except subprocess.CalledProcessError as e:
            print(f"Error downloading land polygons: {e}")
            return None
        except Exception as e:
            print(f"Error processing land polygons: {e}")
            return None

    def clip_tile_to_land(self, tile_path, lon, lat):
        """Clip tile to land polygons preserving 1x1 degree size with 1-pixel extension"""
        # Create temporary directory for clipped land polygons
        clipped_land_dir = os.path.join(self.temp_dir, f"land_{lon}_{lat}")
        os.makedirs(clipped_land_dir, exist_ok=True)

        # Use correct file name without nested directories
        clipped_land_base = os.path.join(self.temp_dir, f"land_{lon}_{lat}")

        try:
            if not self.land_polygons_path or not os.path.exists(self.land_polygons_path):
                print(f"Land polygons not loaded for tile {lon},{lat}")
                return False

            # Get tile dimensions to calculate pixel size
            tile_info = subprocess.run([
                'gdalinfo', tile_path
            ], capture_output=True, text=True, check=True)

            tile_size = None
            for line in tile_info.stdout.split('\n'):
                if 'Size is' in line:
                    dimensions = re.findall(r'Size is (\d+), (\d+)', line)
                    if dimensions:
                        tile_size = int(dimensions[0][0])
                        break

            # Calculate 1-pixel extension in degrees
            if tile_size:
                pixel_size_x = 1.0 / tile_size
                pixel_size_y = 1.0 / tile_size

                # Extend by 1 pixel on each side
                extension_x = pixel_size_x
                extension_y = pixel_size_y
            else:
                # Fallback to small fixed extension if tile size cannot be determined
                extension_x = 0.0001
                extension_y = 0.0001

            # Determine tile bounds (1x1 degree) with 1-pixel extension
            xmin = lon - extension_x
            xmax = lon + 1 + extension_x
            ymin = lat - extension_y
            ymax = lat + 1 + extension_y

            # Create clipped land polygon
            subprocess.run([
                'ogr2ogr',
                '-f', 'ESRI Shapefile',
                '-overwrite',
                '-spat', str(xmin), str(ymin), str(xmax), str(ymax),
                '-clipsrc', str(xmin), str(ymin), str(xmax), str(ymax),
                clipped_land_base,
                self.land_polygons_path
            ], check=True)

            clipped_land_shp = clipped_land_base + '/land_polygons.shp'

            # In debug mode save land polygons information in temp
            if self.debug_mode and os.path.exists(clipped_land_shp):
                land_info_path = os.path.join(self.temp_dir, f"land_polygons_info_{lon}_{lat}.txt")
                subprocess.run([
                    'ogrinfo', '-ro', '-so', '-al', clipped_land_shp
                ], stdout=open(land_info_path, 'w'))
                print(f"Saved land polygons information: {land_info_path}")

            # Check that file exists and is not empty
            if not os.path.exists(clipped_land_shp):
                print(f"No land data for tile {lon},{lat}")
                # In debug mode don't delete temporary files
                if not self.debug_mode:
                    try:
                        shutil.rmtree(clipped_land_dir)
                    except:
                        pass
                    self.cleanup_temp_files([clipped_land_base])
                return True

            # Check feature count in shapefile
            info_result = subprocess.run([
                'ogrinfo', '-ro', '-so', '-q', clipped_land_shp
            ], capture_output=True, text=True)

            # If no features or error occurred - skip clipping
            if info_result.returncode != 0 or 'Feature Count: 0' in info_result.stdout:
                print(f"Empty land polygon for tile {lon},{lat} - skipping clipping")
                # In debug mode save information about empty polygon in temp
                if self.debug_mode:
                    empty_land_info = os.path.join(self.temp_dir, f"empty_land_polygons_{lon}_{lat}.txt")
                    with open(empty_land_info, 'w') as f:
                        f.write(f"Empty land polygon for tile {lon},{lat}\n")
                        f.write(f"Bounds: {xmin}, {ymin}, {xmax}, {ymax}\n")
                    print(f"Saved empty polygon information: {empty_land_info}")
                # In debug mode don't delete temporary files
                if not self.debug_mode:
                    try:
                        shutil.rmtree(clipped_land_dir)
                    except:
                        pass
                    self.cleanup_temp_files([clipped_land_base])
                return True

            # NEW CODE: Check point count in polygon using SQL query
            point_count_result = subprocess.run([
                'ogrinfo', '-dialect', 'SQLite', '-sql',
                f"SELECT SUM(ST_NPoints(geometry)) as total_nodes FROM \"{os.path.splitext(os.path.basename(self.land_polygons_path))[0]}\"",
                clipped_land_shp, '-q'
            ], capture_output=True, text=True)

            point_count = 0
            if point_count_result.returncode == 0:
                # Parse SQL query result
                for line in point_count_result.stdout.split('\n'):
                    if 'total_nodes (Integer) =' in line:
                        try:
                            point_count = int(line.split('=')[1].strip())
                            break
                        except (ValueError, IndexError):
                            pass

            # Skip clipping if mask has exactly 5 points (boundary rectangle + closing point)
            if point_count == 5:
                # In debug mode save mask information
                if self.debug_mode:
                    print(f"Mask contains exactly 5 points (simple rectangle) for tile {lon},{lat} - skipping clipping")
                    rect_mask_info = os.path.join(self.temp_dir, f"rect_mask_{lon}_{lat}.txt")
                    with open(rect_mask_info, 'w') as f:
                        f.write(f"Mask with 5 points for tile {lon},{lat}\n")
                        f.write(f"Bounds: {xmin}, {ymin}, {xmax}, {ymax}\n")
                        f.write(f"Point count: {point_count}\n")
                        f.write(f"SQL query result: {point_count_result.stdout}\n")
                # In debug mode don't delete temporary files
                if not self.debug_mode:
                    try:
                        shutil.rmtree(clipped_land_dir)
                    except:
                        pass
                    self.cleanup_temp_files([clipped_land_base])
                return True

            # Create temporary clipped tile
            temp_tile = os.path.join(self.temp_dir, f"land_clipped_{os.path.basename(tile_path)}")

            # Clip tile by land polygons
            subprocess.run([
                'gdalwarp',
                '-cutline', clipped_land_shp,
                '-co', 'COMPRESS=LZW',
                '-co', 'PREDICTOR=2',
                '-co', 'ZLEVEL=6',
                '-overwrite',
                '-dstnodata', '-9999',
                tile_path, temp_tile
            ], check=True, capture_output=True)

            # Replace source file with clipped one
            shutil.copy2(temp_tile, tile_path)

            # In debug mode don't delete temporary files
            if not self.debug_mode:
                self.cleanup_temp_files([temp_tile])
                try:
                    shutil.rmtree(clipped_land_dir)
                except:
                    pass
                self.cleanup_temp_files([clipped_land_base])
            else:
                print(f"Debug mode: saved intermediate files for tile {lon},{lat}:")
                print(f"  - Land mask: {clipped_land_shp}")
                print(f"  - Temporary clipped tile: {temp_tile}")

            print(f"Tile clipped by land: {os.path.basename(tile_path)}")
            return True

        except subprocess.CalledProcessError as e:
            # If error due to empty polygon - this is normal
            if "Did not get any cutline features" in str(e) or "Feature Count: 0" in str(e):
                print(f"Empty land polygon for tile {lon},{lat} - skipping clipping")
                # In debug mode save information about empty polygon in temp
                if self.debug_mode:
                    empty_land_info = os.path.join(self.temp_dir, f"empty_land_polygons_{lon}_{lat}.txt")
                    with open(empty_land_info, 'w') as f:
                        f.write(f"Empty land polygon for tile {lon},{lat}\n")
                        f.write(f"Bounds: {xmin}, {ymin}, {xmax}, {ymax}\n")
                    print(f"Saved empty polygon information: {empty_land_info}")
                # In debug mode don't delete temporary files
                if not self.debug_mode:
                    try:
                        shutil.rmtree(clipped_land_dir)
                    except:
                        pass
                    self.cleanup_temp_files([clipped_land_base])
                return True
            print(f"Error clipping tile by land {lon},{lat}: {e}")
            # Print full error log
            print(f"Full error output: {e.stderr if hasattr(e, 'stderr') else 'No additional information'}")
            # In debug mode don't delete temporary files
            if not self.debug_mode:
                try:
                    shutil.rmtree(clipped_land_dir)
                except:
                    pass
                self.cleanup_temp_files([clipped_land_base])
            return False
        except Exception as e:
            print(f"Error clipping tile by land {lon},{lat}: {e}")
            # In debug mode don't delete temporary files
            if not self.debug_mode:
                try:
                    shutil.rmtree(clipped_land_dir)
                except:
                    pass
                self.cleanup_temp_files([clipped_land_base])
            return False

    def merge_all_tiffs(self):
        """Merge all TIFF files into single one"""
        merged_tiff = os.path.join(self.working_dir, "france_dem_merged.tif")

        # Skip if file already exists
        if os.path.exists(merged_tiff):
            print(f"Merged file already exists: {merged_tiff}")
            return merged_tiff

        # In debug mode merge only processed files (first 5)
        if self.debug_mode:
            tiff_files = []
            for root, dirs, files in os.walk(self.processed_source_dir):
                for file in files:
                    if file.endswith('.tif'):
                        tiff_files.append(os.path.abspath(os.path.join(root, file)))  # Absolute paths

            # Take only first 5 TIFF files in debug mode
            if len(tiff_files) > 5:
                tiff_files = tiff_files[:5]
                for i, tiff_file in enumerate(tiff_files, 1):
                    print(f"  {i}. {os.path.basename(tiff_file)}")
        else:
            # Normal mode - all files
            tiff_files = []
            for root, dirs, files in os.walk(self.processed_source_dir):
                for file in files:
                    if file.endswith('.tif'):
                        tiff_files.append(os.path.abspath(os.path.join(root, file)))  # Absolute paths

        if not tiff_files:
            print("No TIFF files found for merging")
            return None

        print(f"Merging {len(tiff_files)} TIFF files...")

        # Create VRT from all TIFF files
        merged_vrt = os.path.join(self.working_dir, "merged_dem.vrt")

        try:
            # Create VRT with absolute paths
            subprocess.run(['gdalbuildvrt', merged_vrt] + tiff_files, check=True)

            # Convert VRT to TIFF (without replacing negative values)
            subprocess.run([
                'gdal_translate',
                '-co', 'COMPRESS=LZW',
                '-co', 'PREDICTOR=2',
                '-co', 'ZLEVEL=1',
                '-co', 'BIGTIFF=YES',
                '-co', 'NUM_THREADS=ALL_CPUS',
                '-co', 'TILED=YES',
                '-co', 'BLOCKXSIZE=256',
                '-co', 'BLOCKYSIZE=256',
                '--config', 'GDAL_NUM_THREADS', 'ALL_CPUS',
                '--config', 'GDAL_CACHEMAX', '2048',
                merged_vrt, merged_tiff
            ], check=True, capture_output=True)

            print(f"Merged file created: {merged_tiff}")

            # Delete temporary VRT
            os.remove(merged_vrt)

            return merged_tiff

        except subprocess.CalledProcessError as e:
            print(f"Error merging TIFF files: {e}")
            try:
                if os.path.exists(merged_vrt):
                    os.remove(merged_vrt)
            except:
                pass
            return None
        except Exception as e:
            print(f"Error: {e}")
            return None

    def run(self, skip_download=False, skip_empty_check=False, skip_enhance=False, skip_clip_by_land=False,
            skip_coastline_mask=False, skip_process_archives=False, skip_postprocessing=False):
        """Main method"""
        try:
            # Load tile enhancement list...
            print("Loading tile enhancement list...")
            self.tiles_to_enhance = self.load_tiles_to_enhance()
            if self.tiles_to_enhance is None:
                print("Tile enhancement list not found - will enhance ALL tiles")
            elif self.tiles_to_enhance:
                print(f"Will enhance {len(self.tiles_to_enhance)} tiles")
            else:
                print("Tile enhancement list empty - enhancement skipped")

            # Load tile enhancement ignore list...
            print("Loading tile enhancement ignore list...")
            self.tiles_ignore_enhance = self.load_tiles_ignore_enhance()
            if self.tiles_ignore_enhance:
                print(f"Will ignore enhancement for {len(self.tiles_ignore_enhance)} tiles")
            else:
                print("Tile enhancement ignore list empty - enhancement will be applied to all tiles")

            # Load tile land clipping list...
            print("Loading tile land clipping list...")
            self.tiles_to_clip_by_land = self.load_tiles_to_clip_by_land()
            if self.tiles_to_clip_by_land is None:
                print("Tile land clipping list not found - will clip by land ALL tiles")
            elif self.tiles_to_clip_by_land:
                print(f"Will clip by land {len(self.tiles_to_clip_by_land)} tiles")
            else:
                print("Tile land clipping list empty - land clipping skipped")

            # Load tile coastline mask ignore list...
            self.tiles_ignore_coastline_mask = self.load_tiles_ignore_coastline_mask()
            if self.tiles_ignore_coastline_mask:
                print(f"Will ignore coastline mask for {len(self.tiles_ignore_coastline_mask)} tiles")
            else:
                print("Tile coastline mask ignore list empty - mask will be applied to all tiles")

            # Load tile land clipping ignore list...
            self.tiles_ignore_clip_by_land = self.load_tiles_ignore_clip_by_land()
            if self.tiles_ignore_clip_by_land:
                print(f"Will ignore land clipping for {len(self.tiles_ignore_clip_by_land)} tiles")
            else:
                print("Tile land clipping ignore list empty - clipping will be applied to all tiles")

            # Load land polygons once at the beginning (unless skipped)
            if not skip_clip_by_land:
                print("Loading land polygons...")
                self.land_polygons_path = self.download_land_polygons()
                if self.land_polygons_path:
                    print("Land polygons successfully loaded")
                else:
                    print("Failed to load land polygons, land clipping will be skipped")
            else:
                print("Skipping land polygons download (--skip-clip-by-land)")

            # Load coastline (unless skipped)
            if not skip_coastline_mask:
                print("Loading coastline...")
                coastline_result = self.download_coastline()
                if coastline_result:
                    self.coastline_path = coastline_result
                    print("Coastline successfully loaded")
                else:
                    print("Failed to load coastline, enhancement without coastline mask")
                    self.coastline_path = None
            else:
                print("Skipping coastline download (--skip-coastline-mask)")

            # Rest of the code without changes...
            if skip_download:
                print("Skipping file download, using already downloaded...")
                archive_paths = self.get_existing_archives()
            else:
                links = self.get_file_links()
                if not links:
                    print("No files found")
                    return
                archive_paths = self.download_files_parallel(links)

            if not archive_paths:
                print("No archives to process")
                return

            # Archive processing (unless skipped)
            if not skip_process_archives:
                self.process_all_archives(archive_paths)
            else:
                print("Skipping archive processing (--skip-process-archives)")

            # TIFF file merging...
            merged_tiff = self.merge_all_tiffs()

            # Tile creation...
            if merged_tiff and os.path.exists(merged_tiff):
                min_lon, max_lon, min_lat, max_lat = self.get_tile_bounds(merged_tiff)

                if skip_empty_check:
                    print("Skipping empty tiles check...")
                    valid_tiles = set()
                    for lon in range(min_lon, max_lon):
                        for lat in range(min_lat, max_lat):
                            valid_tiles.add((lon, lat))
                    empty_tiles = set()
                    print(f"Will process all tiles: {len(valid_tiles)}")
                else:
                    valid_tiles, empty_tiles = self.precheck_empty_tiles(merged_tiff, min_lon, max_lon, min_lat,
                                                                         max_lat)

                # High resolution tiles
                print(
                    f"Processing highres tiles ({self.highres_tile_size}x{self.highres_tile_size}) in {self.highres_dir}...")
                self.process_all_tiles(merged_tiff, self.highres_dir, tile_size=self.highres_tile_size,
                                       process_type="highres", prechecked_tiles=(valid_tiles, empty_tiles),
                                       skip_enhance=skip_enhance, skip_clip_by_land=skip_clip_by_land,
                                       skip_coastline_mask=skip_coastline_mask, skip_postprocessing=skip_postprocessing)

                # Low resolution tiles
                print(
                    f"Processing lowres tiles ({self.lowres_tile_size}x{self.lowres_tile_size}) in {self.lowres_dir}...")
                self.process_all_tiles(merged_tiff, self.lowres_dir, tile_size=self.lowres_tile_size,
                                       process_type="lowres", prechecked_tiles=(valid_tiles, empty_tiles),
                                       skip_enhance=skip_enhance, skip_clip_by_land=skip_clip_by_land,
                                       skip_coastline_mask=skip_coastline_mask, skip_postprocessing=skip_postprocessing)

                self.cleanup_temp_directories()
                print(f"Done! Highres tiles in {self.highres_dir}, Lowres tiles in {self.lowres_dir}")

        except Exception as e:
            print(f"Error: {e}")

def main():
    """Main function with command line argument parsing"""
    parser = argparse.ArgumentParser(description='FranceDEM Processor', add_help=False)

    # Required parameters
    parser.add_argument('--working-dir', required=True, help='Working directory for all output files')
    parser.add_argument('--existing-tileset-dir', required=True, help='Directory containing existing lowres tileset')

    # Optional parameters
    parser.add_argument('--max-jobs', type=int, default=None, help='Number of parallel threads (default: 8)')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')

    # Skip parameters
    parser.add_argument('--skip-download', '-s', action='store_true', help='Skip downloading source files')
    parser.add_argument('--skip-process-archives', action='store_true', help='Skip archive processing stage')
    parser.add_argument('--skip-empty-check', '-e', action='store_true', help='Skip empty tiles check')
    parser.add_argument('--skip-enhance', action='store_true', help='Skip tile enhancement stage')
    parser.add_argument('--skip-coastline-mask', action='store_true', help='Skip coastline mask stage')
    parser.add_argument('--skip-clip-by-land', action='store_true', help='Skip land clipping stage')
    parser.add_argument('--skip-postprocessing', action='store_true', help='Skip postprocessing stage')

    # Help
    parser.add_argument('--help', '-h', action='store_true', help='Show this help message')

    args = parser.parse_args()

    if args.help:
        processor = FranceDEMProcessor()
        processor.print_help()
        return

    # Validate required parameters
    if not args.working_dir or not args.existing_tileset_dir:
        print("Error: --working-dir and --existing-tileset-dir are required parameters")
        processor = FranceDEMProcessor()
        processor.print_help()
        return

    # Create processor instance
    processor = FranceDEMProcessor(
        debug_mode=args.debug,
        working_dir=args.working_dir,
        existing_tileset_dir=args.existing_tileset_dir,
        max_jobs=args.max_jobs
    )

    # Run processing
    processor.run(
        skip_download=args.skip_download,
        skip_empty_check=args.skip_empty_check,
        skip_enhance=args.skip_enhance,
        skip_clip_by_land=args.skip_clip_by_land,
        skip_coastline_mask=args.skip_coastline_mask,
        skip_process_archives=args.skip_process_archives,
        skip_postprocessing=args.skip_postprocessing
    )

if __name__ == "__main__":
    main()