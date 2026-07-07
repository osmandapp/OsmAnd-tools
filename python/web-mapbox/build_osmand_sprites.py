#!/usr/bin/env python3
import os
import shutil
import subprocess
import sys

SRC_DIRS = [
    '/home/xmd5a/git/OsmAnd-resources/rendering_styles/style-icons/map-icons-svg',
    '/home/xmd5a/git/OsmAnd-resources/rendering_styles/style-icons/map-shaders-svg'
]
DST_DIR = '/mnt/wd_2tb/mvt/openmaptiles/style/icons/'
WORK_DIR = '/mnt/wd_2tb/mvt/openmaptiles/'
REPO_DIR = '/home/xmd5a/git/osmand-subst/'
UTILITIES_SH = '/home/xmd5a/utilites/OsmAndMapCreator-main/utilities.sh'
ADDITION_DIR = '/home/xmd5a/git/OsmAnd-resources/rendering_styles/mvt-icons-addition'

PREFIXES = ['mx_', 'c_mx_', 'c_h_', 'h_']
EXCLUDE_PATTERNS = ['seamark', "topo_"]


def main():
    if os.path.exists(DST_DIR):
        print(f"Cleaning existing directory: {DST_DIR}")
        shutil.rmtree(DST_DIR)
    os.makedirs(DST_DIR)
    print(f"Created fresh directory: {DST_DIR}")

    for src_dir in SRC_DIRS:
        if not os.path.isdir(src_dir):
            continue

        is_shader_dir = "map-shaders-svg" in src_dir

        for f in os.listdir(src_dir):
            if not f.lower().endswith('.svg'):
                continue
            if any(p in f for p in EXCLUDE_PATTERNS):
                continue

            src = os.path.join(src_dir, f)
            if not os.path.isfile(src):
                continue

            original_filename = f

            for p in PREFIXES:
                if f.startswith(p):
                    f = f[len(p):]
                    break

            dst_path = os.path.join(DST_DIR, f)

            # Resize only from map-shaders-svg, without "shield" and without "osmc"
            if (is_shader_dir and 
                "shield" not in original_filename.lower() and 
                "osmc" not in original_filename.lower()):
                
                print(f"Resizing to 70%: {f} (from shaders)")
                try:
                    # Get original dimensions
                    identify_cmd = f"identify -format '%w %h' '{src}'"
                    result = subprocess.run(identify_cmd, shell=True, capture_output=True, text=True, executable='/bin/bash')
                    if result.returncode == 0:
                        width, height = map(int, result.stdout.strip().split())
                        new_width = max(1, int(width * 0.5))
                        new_height = max(1, int(height * 0.5))
                        
                        # Keep output as SVG
                        cmd = (
                            f"inkscape '{src}' --export-type=svg --export-filename='{dst_path}' "
                            f"--export-width={new_width} --export-height={new_height} --export-area-page 2>/dev/null || "
                            f"rsvg-convert -w {new_width} -h {new_height} --format=svg '{src}' -o '{dst_path}' || "
                            f"cp '{src}' '{dst_path}'"
                        )
                        subprocess.run(cmd, shell=True, check=True, executable='/bin/bash')
                    else:
                        raise ValueError("Could not get dimensions")
                except Exception as e:
                    print(f"Warning: Resize failed for {f}, copying original. Error: {e}")
                    shutil.copy2(src, dst_path)
            else:
                shutil.copy2(src, dst_path)

    # Run OsmAnd MapCreator utilities to generate MVT icons
    print("Running generate-mvt-icons...")
    generate_cmd = f"export repo_dir={REPO_DIR} && {UTILITIES_SH} generate-mvt-icons {DST_DIR} --shield-size=34"
    subprocess.run(generate_cmd, shell=True, check=True, executable='/bin/bash')

    # Copy additional icons with overwrite
    if os.path.exists(ADDITION_DIR):
        print(f"Copying additional icons from {ADDITION_DIR} to {DST_DIR} with overwrite")
        for f in os.listdir(ADDITION_DIR):
            if f.lower().endswith('.svg'):
                src = os.path.join(ADDITION_DIR, f)
                dst = os.path.join(DST_DIR, f)
                if os.path.isfile(src):
                    shutil.copy2(src, dst)
                    print(f"Copied (overwritten): {f}")
    else:
        print(f"Warning: Addition directory {ADDITION_DIR} does not exist")

    os.chdir(WORK_DIR)
    cmd = "docker compose run --rm --user=$(id -u):$(id -g) openmaptiles-tools bash -c 'spreet /style/icons build/style/sprite && spreet --retina /style/icons build/style/sprite@2x'"
    subprocess.run(cmd, shell=True, check=True, executable='/bin/bash')


if __name__ == '__main__':
    main()