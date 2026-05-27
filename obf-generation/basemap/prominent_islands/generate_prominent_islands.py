#!/usr/bin/env python3
import subprocess
import argparse
import sys
import os
import xml.etree.ElementTree as ET

# This script downloads island data using remote overpass instance
# Typical run parameters:
# --host builder.osmand.net --user .. --key /home/../.ssh/id_rsa --output islands

def fix_xml_version(filepath):
    print(f"🔧 Fixing versions in {filepath}...")
    tree = ET.parse(filepath)
    root = tree.getroot()

    changed = False
    for elem in list(root.findall('.//node')) + list(root.findall('.//way')) + list(root.findall('.//relation')):
        if 'version' not in elem.attrib:
            elem.set('version', '1')
            changed = True

    if changed:
        tree.write(filepath, encoding='utf-8', xml_declaration=True)
        print(f"✅ Versions were added")
    else:
        print(f"✅ Versions already fixed")


def run_overpass_query(ssh_command, query, output_file, timeout):
    if os.path.exists(output_file):
        print(f"⏭️  File already exists: {output_file}")
        return True

    print(f"📥 Downloading {output_file}...")

    try:
        proc = subprocess.Popen(ssh_command, stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True, encoding="utf-8")

        stdout, stderr = proc.communicate(input=query, timeout=timeout + 60)

        if proc.returncode != 0:
            print(f"❌ Error: {stderr}", file=sys.stderr)
            return False

        if len(stdout.strip()) < 100:
            print(f"⚠️  Empty result for {output_file}")
            return False

        with open(output_file, "w", encoding="utf-8") as f:
            f.write(stdout)

        print(f"✅ Saved: {output_file} ({len(stdout) / (1024*1024):.1f} Mb)")
        return True

    except Exception as e:
        print(f"❌ Error: {e}", file=sys.stderr)
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--key", required=True)
    parser.add_argument("--output", default="islands")
    parser.add_argument("--timeout", type=int, default=1800)
    args = parser.parse_args()

    ssh_command = ["ssh", "-i", args.key, f"{args.user}@{args.host}",
                   "/home/overpass/osm3s/bin/osm3s_query"]

    queries = {
        "nodes": f'''[out:xml][timeout:{args.timeout}];
node["place"="island"];
out meta geom;''',

        "ways": f'''[out:xml][timeout:{args.timeout}];
way["place"="island"];
out body geom;
>;
out skel geom;''',

        "relations": f'''[out:xml][maxsize:5000000000][timeout:{args.timeout}];
relation["place"="island"];
out body geom;
>>;
out skel geom;'''
    }

    for geom_type, query in queries.items():
        output_file = f"{args.output}_{geom_type}.osm"
        if run_overpass_query(ssh_command, query, output_file, args.timeout):
            fix_xml_version(output_file)

    print("\n✅ Done!")


if __name__ == "__main__":
    main()