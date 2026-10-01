#!/usr/bin/env python3
"""Install a local Valhalla router for turn lanes comparison.

    python3 build_valhalla.py [--dir ../valhalla/] [--version 3.9.0]

Valhalla is taken from the pyvalhalla wheel: it ships the prebuilt C++ executables (valhalla_build_tiles,
valhalla_build_admins, valhalla_service, ...) for macOS arm64 and Linux, so no C++ toolchain is needed.
Everything lands in --dir:
    <dir>/venv    python virtual env with pyvalhalla
    <dir>/data    tiles built by start_server.py, one folder per .pbf
"""
import argparse
import os
import subprocess
import sys
import venv

DEFAULT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', '..', 'valhalla')
EXECUTABLES = ('valhalla_build_tiles', 'valhalla_build_admins', 'valhalla_service')


def venv_python(root):
    return os.path.join(root, 'venv', 'Scripts' if os.name == 'nt' else 'bin', 'python')


def bin_dir(root):
    """Directory of the pyvalhalla C++ executables, or None when Valhalla is not installed in root."""
    python = venv_python(root)
    if not os.path.exists(python):
        return None
    res = subprocess.run([python, '-m', 'valhalla', 'print_bin_path'], capture_output=True, text=True)
    return res.stdout.strip() if res.returncode == 0 else None


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--dir', default=DEFAULT_DIR, help='where to install Valhalla (default: %(default)s)')
    parser.add_argument('--version', help='pyvalhalla version, latest when omitted')
    args = parser.parse_args()

    root = os.path.abspath(args.dir)
    os.makedirs(os.path.join(root, 'data'), exist_ok=True)
    python = venv_python(root)
    if not os.path.exists(python):
        print('Creating virtual env in %s' % os.path.join(root, 'venv'))
        venv.create(os.path.join(root, 'venv'), with_pip=True)
    package = 'pyvalhalla' + ('==' + args.version if args.version else '')
    subprocess.run([python, '-m', 'pip', 'install', '-q', '--upgrade', 'pip'], check=True)
    subprocess.run([python, '-m', 'pip', 'install', '-q', '--upgrade', package], check=True)

    bins = bin_dir(root)
    missing = [e for e in EXECUTABLES if not bins or not os.path.exists(os.path.join(bins, e))]
    if missing:
        sys.exit('pyvalhalla has no %s for this platform' % ', '.join(missing))
    version = subprocess.run([python, '-m', 'valhalla', '--version'], capture_output=True, text=True).stdout.strip()
    print('Valhalla %s installed, executables in %s' % (version, bins))
    print('Next: python3 %s /path/to/region.osm.pbf%s' % (
        os.path.join(os.path.dirname(os.path.abspath(__file__)), 'start_server.py'),
        '' if os.path.abspath(DEFAULT_DIR) == root else ' --dir ' + args.dir))


if __name__ == '__main__':
    main()
