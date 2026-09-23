#!/usr/bin/env python3
"""Build Valhalla tiles for a .pbf and serve them on localhost.

    python3 start_server.py /path/to/region.osm.pbf [--dir ../valhalla/] [--port 8002] [--rebuild]
    python3 start_server.py --clean /path/to/region.osm.pbf   removes the tiles built for it (the .pbf stays)

Needs Valhalla installed by build_valhalla.py into the same --dir. Tiles are built once per .pbf into
<dir>/data/<pbf name>/ and rebuilt only when the .pbf changes (or with --rebuild).
Admin polygons come from the same .pbf, so the extract needs its country boundary for Valhalla to know
the driving side (left-hand traffic in Australia, UK, ...).

Then ask for lanes in OSRM format, as the public server does:
    curl -s localhost:8002/route -d '{"locations":[{"lat":..,"lon":..},{"lat":..,"lon":..}],
                                      "costing":"auto","format":"osrm","banner_instructions":true}'
Ctrl+C stops the server.
"""
import argparse
import json
import os
import shutil
import socket
import subprocess
import sys

from build_valhalla import DEFAULT_DIR, bin_dir, venv_python


def pbf_name(pbf):
    name = os.path.basename(pbf)
    for ext in ('.osm.pbf', '.pbf'):
        if name.endswith(ext):
            return name[:-len(ext)]
    return name


def run(cmd, log):
    print('> %s' % ' '.join(os.path.basename(c) if i == 0 else c for i, c in enumerate(cmd)), flush=True)
    with open(log, 'a') as f:
        res = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT)
    if res.returncode != 0:
        sys.exit('%s failed, see %s' % (os.path.basename(cmd[0]), log))


def build(root, bins, pbf, data, port, threads):
    tiles = os.path.join(data, 'tiles')
    config = os.path.join(data, 'valhalla.json')
    log = os.path.join(data, 'build.log')
    # tiles of a previous build that the new .pbf doesn't cover would otherwise stay and keep being served
    shutil.rmtree(tiles, ignore_errors=True)
    os.makedirs(tiles)
    if os.path.exists(log):
        os.remove(log)
    # paths of optional data we don't build (timezones, elevation, ...) point into data/, missing files are skipped
    run([venv_python(root), '-m', 'valhalla.valhalla_build_config',
         '--mjolnir-tile-dir', tiles,
         '--mjolnir-admin', os.path.join(data, 'admin.sqlite'),
         '--mjolnir-timezone', os.path.join(data, 'tz_world.sqlite'),
         '--mjolnir-landmarks', os.path.join(data, 'landmarks.sqlite'),
         '--additional-data-elevation', os.path.join(data, 'elevation'),
         '--mjolnir-concurrency', str(threads),
         '--output', config], log)
    set_port(config, port)
    run([os.path.join(bins, 'valhalla_build_admins'), '-c', config, pbf], log)
    run([os.path.join(bins, 'valhalla_build_tiles'), '-c', config, pbf], log)
    # stamp last, so an interrupted build is redone next time
    with open(os.path.join(data, 'source.json'), 'w') as f:
        json.dump({'pbf': pbf, 'mtime': os.path.getmtime(pbf)}, f)
    return config


def set_port(config, port):
    with open(config) as f:
        cfg = json.load(f)
    cfg['httpd']['service']['listen'] = 'tcp://127.0.0.1:%d' % port
    # tiles are read from tile_dir; without this the service logs an error per thread for the missing tar files
    cfg['mjolnir'].pop('tile_extract', None)
    cfg['mjolnir'].pop('traffic_extract', None)
    with open(config, 'w') as f:
        json.dump(cfg, f, indent=2)


def is_built(pbf, data):
    try:
        with open(os.path.join(data, 'source.json')) as f:
            src = json.load(f)
    except (OSError, ValueError):
        return False
    return src.get('pbf') == pbf and src.get('mtime') == os.path.getmtime(pbf)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('pbf', help='OSM extract (.osm.pbf) to route on')
    parser.add_argument('--dir', default=DEFAULT_DIR, help='Valhalla dir from build_valhalla.py (default: %(default)s)')
    parser.add_argument('--port', type=int, default=8002)
    parser.add_argument('--threads', type=int, default=os.cpu_count() or 4)
    parser.add_argument('--rebuild', action='store_true', help='rebuild tiles even if they are up to date')
    parser.add_argument('--clean', action='store_true',
                        help='remove the tiles built for the .pbf and exit; the .pbf itself may be gone already')
    args = parser.parse_args()

    root = os.path.abspath(args.dir)
    pbf = os.path.abspath(args.pbf)
    if args.clean:
        data = os.path.join(root, 'data', pbf_name(pbf))
        if not os.path.isdir(data):
            sys.exit('Nothing built for %s in %s' % (os.path.basename(pbf), os.path.join(root, 'data')))
        shutil.rmtree(data)
        print('Removed %s' % data)
        return
    if not os.path.isfile(pbf):
        sys.exit('No such file: %s' % pbf)
    bins = bin_dir(root)
    if not bins:
        sys.exit('Valhalla is not installed in %s, run build_valhalla.py --dir %s first' % (root, args.dir))

    with socket.socket() as sock:
        if sock.connect_ex(('127.0.0.1', args.port)) == 0:
            sys.exit('Port %d is busy (another start_server.py?), stop it or pass --port' % args.port)

    data = os.path.join(root, 'data', pbf_name(pbf))
    config = os.path.join(data, 'valhalla.json')
    if args.rebuild or not is_built(pbf, data):
        print('Building tiles for %s into %s' % (os.path.basename(pbf), data))
        config = build(root, bins, pbf, data, args.port, args.threads)
    else:
        print('Tiles for %s are up to date' % os.path.basename(pbf))
        set_port(config, args.port)

    print('Valhalla serves %s on http://localhost:%d (Ctrl+C to stop)' % (os.path.basename(pbf), args.port), flush=True)
    try:
        subprocess.run([os.path.join(bins, 'valhalla_service'), config, str(args.threads)])
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
