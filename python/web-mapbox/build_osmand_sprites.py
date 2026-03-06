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
PREFIXES = ['mx_', 'c_mx_', 'c_h_', 'h_']
EXCLUDE_PATTERNS = ['seamark', "topo_"]

def main():
    if os.path.exists(DST_DIR):
        shutil.rmtree(DST_DIR)
    os.makedirs(DST_DIR)

    for src_dir in SRC_DIRS:
        if not os.path.isdir(src_dir):
            continue
        for f in os.listdir(src_dir):
            if not f.lower().endswith('.svg'):
                continue
            if any(p in f for p in EXCLUDE_PATTERNS):
                continue
            src = os.path.join(src_dir, f)
            if not os.path.isfile(src):
                continue
            for p in PREFIXES:
                if f.startswith(p):
                    f = f[len(p):]
                    break
            shutil.copy2(src, os.path.join(DST_DIR, f))

    os.chdir(WORK_DIR)
    cmd = "docker compose run --rm --user=$(id -u):$(id -g) openmaptiles-tools bash -c 'spreet /style/icons build/style/sprite && spreet --retina /style/icons build/style/sprite@2x'"
    subprocess.run(cmd, shell=True, check=True, executable='/bin/bash')

if __name__ == '__main__':
    main()