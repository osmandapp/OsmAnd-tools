import csv
import argparse
import sys
from xml.sax.saxutils import escape

def safe_escape(value):
    """
    Escapes a string only if it does not already contain escaped characters
    """
    # Check for already escaped sequences
    if isinstance(value, str):
        if '&amp;' in value or '<' in value or '>' in value or '&quot;' in value or '&apos;' in value:
            return value  # The string is already escaped
        return escape(value)  # Escape the string if it is not escaped
    return str(value)

def create_node_str(node_id, lat, lon, street, city, postcode, housenumber=None, add_tags=False):
    parts = [f'<node id="{node_id}" lat="{lat}" lon="{lon}">']
    
    if add_tags:
        for k, v in [('addr:street', street), ('addr:city', city), ('addr:postcode', postcode)]:
            parts.append(f'  <tag k="{safe_escape(k)}" v="{safe_escape(v)}" />')
        
        if housenumber:
            parts.append(f'  <tag k="addr:housenumber" v="{safe_escape(str(housenumber))}" />')
        
        parts.append(f'  <tag k="tiger:osmand" v="yes" />')
    
    parts.append('</node>')
    return '\n'.join(parts)

# Generate OSM XML from CSV with streaming recording
def generate_osm_from_csv(input_csv, output_osm):
    # Calc row count
    with open(input_csv, newline='') as csvfile:
        total_rows = sum(1 for _ in csvfile) - 1  # Учитываем заголовок файла
    
    with open(input_csv, newline='') as csvfile, open(output_osm, 'w', encoding='utf-8') as outfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        
        outfile.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        outfile.write('<osm version="0.6" generator="python-csv-to-osm">\n')
        
        node_id = -10000  # Start id for node
        way_id = 1        # Start id for way

        processed_rows = 0
        last_reported_percent = 0

        # Parse CSV
        for row in reader:
            street = safe_escape(row['street'])
            city = safe_escape(row['city'])
            postcode = safe_escape(row['postcode'])
            interpolation = safe_escape(row['interpolation'])
            geometry = row['geometry']  # Geometry does not require escaping

            coords = geometry.replace('LINESTRING(', '').replace(')', '').split(',')
            nodes = []

            # Create nodes for the first, intermediate and last point
            for idx, coord in enumerate(coords):
                lon, lat = map(float, coord.split())
                housenumber = None
                add_tags = False

                # Add tags only for the first and last point
                if idx == 0:
                    housenumber = row['from']
                    add_tags = True
                elif idx == len(coords) - 1:
                    housenumber = row['to']
                    add_tags = True

                # Create a node and immediately write it to a file
                node_str = create_node_str(node_id, lat, lon, street, city, postcode, housenumber, add_tags)
                outfile.write(node_str + '\n')

                nodes.append(node_id)
                node_id -= 1

            outfile.write(f'<way id="{way_id}" version="1">\n')
            way_id += 1
            
            for node_ref in nodes:
                outfile.write(f'  <nd ref="{node_ref}" />\n')
            
            outfile.write(f'  <tag k="addr:interpolation" v="{interpolation}" />\n')
            outfile.write(f'  <tag k="tiger:osmand" v="yes" />\n')
            outfile.write('</way>\n')

            processed_rows += 1
            current_percent = int((processed_rows / total_rows) * 100)

            if current_percent > last_reported_percent and current_percent % 10 == 0:  # Each 10%
                print(f"Progress: {current_percent}%")
                last_reported_percent = current_percent
                sys.stdout.flush()

        outfile.write('</osm>\n')

def main():
    parser = argparse.ArgumentParser(description='Convert CSV with interpolated addresses to OSM XML format.')
    parser.add_argument('input', type=str, help='Input CSV file with interpolated addresses')
    parser.add_argument('output', type=str, help='Output OSM XML file')
    
    args = parser.parse_args()

    generate_osm_from_csv(args.input, args.output)

if __name__ == '__main__':
    main()