#!/usr/bin/env python3
import os
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET

SRC_DIRS = [
    '/home/xmd5a/git/OsmAnd-resources/rendering_styles/style-icons/map-icons-svg',
    '/home/xmd5a/git/OsmAnd-resources/rendering_styles/style-icons/map-shaders-svg'
]
DST_DIR = '/mnt/wd_2tb/mvt/openmaptiles/style/icons/'
WORK_DIR = '/mnt/wd_2tb/mvt/openmaptiles/'
REPO_DIR = '/home/xmd5a/git/osmand-subst/'
UTILITIES_SH = '/home/xmd5a/utilites/OsmAndMapCreator-main/utilities.sh'
ADDITION_DIR = '/home/xmd5a/git/OsmAnd-resources/mvt/icons-addition'

PREFIXES = ['mx_', 'c_mx_', 'c_h_', 'h_']
EXCLUDE_PATTERNS = ['seamark', "topo_"]
SCALE_FACTOR = 0.7

def get_svg_dimensions(svg_path):
    """Получить размеры SVG из XML, без ImageMagick."""
    try:
        tree = ET.parse(svg_path)
        root = tree.getroot()
        ns = {'svg': 'http://www.w3.org/2000/svg'}
        
        width = root.get('width')
        height = root.get('height')
        viewBox = root.get('viewBox')
        
        if width and height:
            # Убираем единицы измерения (px, pt и т.д.)
            w = float(''.join(c for c in width if c.isdigit() or c == '.'))
            h = float(''.join(c for c in height if c.isdigit() or c == '.'))
            return w, h
        elif viewBox:
            parts = viewBox.split()
            if len(parts) == 4:
                return float(parts[2]), float(parts[3])
    except Exception as e:
        print(f"Warning: Could not parse SVG dimensions for {svg_path}: {e}")
    
    return None, None


def scale_svg_with_rsvg(src, dst, scale):
    """Масштабировать SVG через rsvg-convert с --zoom."""
    try:
        cmd = [
            'rsvg-convert',
            '--format=svg',
            f'--zoom={scale}',
            src,
            '-o', dst
        ]
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Warning: rsvg-convert failed: {e.stderr}")
        return False


def scale_svg_with_inkscape(src, dst, scale):
    """Масштабировать SVG через Inkscape actions (fallback)."""
    try:
        # Используем actions для масштабирования
        cmd = [
            'inkscape',
            '--batch-process',
            '--actions',
            f'transform-scale:{scale};FileSave;FileClose',
            src,
            '-o', dst
        ]
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Warning: Inkscape scale failed: {e.stderr}")
        return False


def scale_svg_manually(src, dst, scale):
    """Масштабировать SVG вручную через XML (last resort)."""
    try:
        tree = ET.parse(src)
        root = tree.getroot()
        
        # Добавляем/изменяем viewBox и убираем width/height
        # или масштабируем через transform
        ns = {'svg': 'http://www.w3.org/2000/svg'}
        
        # Создаём группу с масштабированием
        g = ET.Element('{http://www.w3.org/2000/svg}g')
        g.set('transform', f'scale({scale})')
        
        # Перемещаем все дочерние элементы в группу
        for child in list(root):
            if child.tag not in ('{http://www.w3.org/2000/svg}defs',
                                '{http://www.w3.org/2000/svg}metadata',
                                '{http://www.w3.org/2000/svg}namedview',
                                '{http://www.sodipodi.org/2000/sodipodi}namedview'):
                g.append(child)
                root.remove(child)
        
        root.append(g)
        tree.write(dst, encoding='utf-8', xml_declaration=True)
        return True
    except Exception as e:
        print(f"Warning: Manual scale failed: {e}")
        return False


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
                
                print(f"Resizing: {f} (from shaders, scale={SCALE_FACTOR})")
                
                # Пробуем rsvg-convert (лучший вариант)
                if scale_svg_with_rsvg(src, dst_path, SCALE_FACTOR):
                    print(f"  -> Scaled with rsvg-convert")
                # Fallback на Inkscape
                elif scale_svg_with_inkscape(src, dst_path, SCALE_FACTOR):
                    print(f"  -> Scaled with Inkscape")
                # Fallback на ручное масштабирование
                elif scale_svg_manually(src, dst_path, SCALE_FACTOR):
                    print(f"  -> Scaled manually via XML")
                else:
                    print(f"Warning: All scaling methods failed for {f}, copying original")
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