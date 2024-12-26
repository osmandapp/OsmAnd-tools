""" Script for generating prominent_peaks.osm.
Requires prominent_peaks_kmz.osm with synthetic map of peaks (name+ele) (originally exported from kmz (http://peaklist.org/ultras.html))."""
import os
import requests
import math
import xml.etree.ElementTree as ET
import subprocess
import gzip
import shutil

PEAKS = "peaks_above_1500m_osm.osm"
PROMINENT_PEAKS_INPUT = "prominent_peaks_kmz.osm"
NEAREST_PEAKS = "nearest_peaks.osm"
UNMATCHED_PEAKS = "unmatched_peaks.osm"
PROMINENT_PEAKS = "points_prominent_peaks.osm"
USE_LOCAL_OVERPASS = True
LOCAL_OVERPASS_PATH = "/home/overpass/osm3s/bin/osm3s_query"
REMOTE_OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OVERPASS_API_PATH = LOCAL_OVERPASS_PATH if USE_LOCAL_OVERPASS else REMOTE_OVERPASS_URL
SEARCH_RADIUS = 7000  # Search radius in meters

def download_osm_data(query, output_file):
    """
    Downloads data via Overpass API (local or remote) and saves it to XML file.
    """
    if USE_LOCAL_OVERPASS:
        print("Using local Overpass API...")
        try:
            result = subprocess.run(
                [OVERPASS_API_PATH],
                input=query,
                text=True,
                capture_output=True,
                check=True
            )
            with open(output_file, "w") as f:
                f.write(result.stdout)
            print(f"Data successfully saved to file: {output_file}")
        except subprocess.CalledProcessError as e:
            print(f"Error loading data from local Overpass: {e.stderr}")
            raise
    else:
        print("Request to remote Overpass API...")
        response = requests.post(REMOTE_OVERPASS_URL, data={"data": query})
        if response.status_code == 200:
            with open(output_file, "wb") as f:
                f.write(response.content)
            print(f"Data successfully saved to file: {output_file}")
        else:
            print(f"Error loading data from remote Overpass API: {response.status_code}")
            raise ValueError("Unable to get data from Overpass API")

def parse_nodes_from_osm(file_path):
    """
    Extracts information about vertices (nodes) from an OSM file, including their IDs.
    """
    tree = ET.parse(file_path)
    root = tree.getroot()

    nodes = []
    for node in root.findall("node"):
        lat = float(node.attrib["lat"])
        if lat < -85:  # Skipping peaks souther than -85 lat
            continue
        node_id = node.attrib.get("id", None)
        lat = float(node.attrib["lat"])
        lon = float(node.attrib["lon"])
        tags = {tag.attrib["k"]: tag.attrib["v"] for tag in node.findall("tag")}
        nodes.append({"id": node_id, "lat": lat, "lon": lon, "tags": tags})
    return nodes

def haversine(lat1, lon1, lat2, lon2):
    """
    Calculates the distance between two coordinates (Harversine formula).
    """
    R = 6371000  # Earth radius in meters
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def clean_ele(ele_value):
    """
    Clears the value of ele, leaving only the numeric (integer) part.
    Example: "1500.5м" -> 1500, "2000,12" -> 2000, "абс1234м" -> 1234.
    """
    if not ele_value:
        return 0
    # Remove all characters except numbers and decimal separators
    clean_value = ''.join(c if c.isdigit() else ' ' for c in ele_value.replace(",", "."))
    # Taking the first numerical part before the point
    return int(clean_value.split()[0]) if clean_value.split() else 0

def filter_peaks_within_bbox(peaks, center_lat, center_lon, radius_km):
    """
    Filters peaks, leaving only those within the bbox radius.
    """
    delta_lat = radius_km / 111.32  # 1° latitude ≈ 111.32 km
    delta_lon = radius_km / (111.32 * math.cos(math.radians(center_lat)))

    min_lat = center_lat - delta_lat
    max_lat = center_lat + delta_lat
    min_lon = center_lon - delta_lon
    max_lon = center_lon + delta_lon

    # Selecting only peaks within the specified bounding box
    filtered_peaks = [
        peak for peak in peaks
        if min_lat <= peak['lat'] <= max_lat and min_lon <= peak['lon'] <= max_lon
    ]
    return filtered_peaks

def find_nearest_peaks(prominent_peaks, peaks, radius):
    """
    Finds the closest peaks to each point in the prominent_peaks list within a given radius.
    If ele is empty, the height check is skipped.
    """
    nearest_peaks = []

    for prom_peak in prominent_peaks:
        nearest = None
        min_distance = radius
        min_height_diff = float("inf")

        # Original and processed (cleaned) ele at the reference peak
        prom_peak_ele_orig = prom_peak["tags"].get("ele", "")  # Original value
        prom_peak_ele = clean_ele(prom_peak_ele_orig) if prom_peak_ele_orig else None  # Processed value

        for peak in peaks:
            peak_ele_orig = peak["tags"].get("ele", "")
            peak_ele = clean_ele(peak_ele_orig) if peak_ele_orig else None

            distance = haversine(prom_peak["lat"], prom_peak["lon"], peak["lat"], peak["lon"])

            # Checking the distance if one of the ele is empty
            if prom_peak_ele is None or peak_ele is None:
                if distance < min_distance:
                    min_distance = distance
                    nearest = peak
            else:
                # If both ele are present, also checking for the difference in heights
                height_diff = abs(prom_peak_ele - peak_ele)
                if distance < min_distance or (distance == min_distance and height_diff < min_height_diff):
                    min_distance = distance
                    min_height_diff = height_diff
                    nearest = peak

        if nearest:
            nearest_peaks.append(nearest)

    return nearest_peaks

def write_to_osm(output_file, nodes):
    """
    Writes a list of nodes to an OSM XML file, indented for better readability.
    """
    root = ET.Element("osm", version="0.6", generator="nearest_peaks_script")

    for node in nodes:
        node_id = node.get("id", str(-1))  # Use id if available
        node_element = ET.Element(
            "node",
            id=node_id,
            lat=str(node["lat"]),
            lon=str(node["lon"]),
            version="1",
        )
        for key, value in node["tags"].items():
            ET.SubElement(node_element, "tag", k=key, v=value)  # Original key-value
        # Adding osmand_prominent_peak tag
        ET.SubElement(node_element, "tag", k="osmand_prominent_peak", v="yes")
        root.append(node_element)

    # Formatting for readability
    indent_xml(root)

    # Save the file with the declaration
    tree = ET.ElementTree(root)
    with open(output_file, "wb") as f:
        tree.write(f, encoding="utf-8", xml_declaration=True)

def indent_xml(elem, level=0):
    """
    Adds indentation for XML tree formatting.
    """
    i = "\n" + "  " * level
    if len(elem):  # If the element has child nodes
        if not elem.text or not elem.text.strip():
            elem.text = i + "  "
        for child in elem:
            indent_xml(child, level + 1)
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
    else:  # If the element has no child nodes
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i

def add_tag_to_unmatched_peaks(file_path):
    """
    Adds the 'natural=peak' tag to all nodes in the OSM file unmatched_peaks.osm.
    """
    tree = ET.parse(file_path)
    root = tree.getroot()

    for node in root.findall("node"):
        # Checking if there is already a `natural` tag, if not, adding it
        existing_natural_tag = node.find("tag[@k='natural']")
        if not existing_natural_tag:
            ET.SubElement(node, "tag", k="natural", v="peak")

    # Save the file back
    tree.write(file_path, encoding="utf-8", xml_declaration=True)

def combine_osm_files(output_file, *input_files):
    """
    Combines multiple OSM files into one.
    """
    root = ET.Element("osm", version="0.6", generator="combine_osm_script")

    for input_file in input_files:
        tree = ET.parse(input_file)
        input_root = tree.getroot()

        # Add all nodes from the current file to the final one
        for node in input_root.findall("node"):
            root.append(node)

    # Formatting for readability
    indent_xml(root)

    # Save the resulting file
    tree = ET.ElementTree(root)
    with open(output_file, "wb") as f:
        tree.write(f, encoding="utf-8", xml_declaration=True)
    print(f"The combined file was saved as {output_file}")

if __name__ == "__main__":
    # 1. If the peak file does not exist, download the data from the Overpass API
    if not os.path.exists(PEAKS):
        overpass_query = """
        [out:xml][timeout:2500];
	(
        node["natural"="peak"];
        node["natural"="volcano"];
	);
        out body;
        """
        download_osm_data(overpass_query, PEAKS)

    # 2. If the file with prominent peaks does not exist then report an error
    if not os.path.exists(PROMINENT_PEAKS_INPUT):
        raise FileNotFoundError(f"File {PROMINENT_PEAKS_INPUT} not found.")

    # 3. Reading OSM files
    print("Reading data from OSM files...")
    peaks = parse_nodes_from_osm(PEAKS)
    prominent_peaks = parse_nodes_from_osm(PROMINENT_PEAKS_INPUT)

    nearest_peaks = []
    unmatched_peaks = []  # List for peaks that could not be matched
    search_radius_km = 10  # Radius in kilometers for bounding box (each side)

    for prom_peak in prominent_peaks:
        center_lat = prom_peak["lat"]
        center_lon = prom_peak["lon"]

        # Filtering peaks
        local_peaks = filter_peaks_within_bbox(peaks, center_lat, center_lon, search_radius_km)

        # Find the closest peak within this subset
        result = find_nearest_peaks([prom_peak], local_peaks, SEARCH_RADIUS)
        if result:
            nearest_peaks.extend(result)
        else:
            unmatched_peaks.append(prom_peak)  # Add a peak without a match
    write_to_osm(UNMATCHED_PEAKS, unmatched_peaks)
    # 5. Saving results to OSM XML file
    write_to_osm(NEAREST_PEAKS, nearest_peaks)
    # Adding tags to unmatched_peaks.osm
    add_tag_to_unmatched_peaks(UNMATCHED_PEAKS)

    # Combining files nearest_peaks.osm and unmatched_peaks.osm to prominent_peaks.osm
    combine_osm_files(PROMINENT_PEAKS, NEAREST_PEAKS, UNMATCHED_PEAKS)
    with open(PROMINENT_PEAKS, "rb") as f_in:
        with gzip.open(PROMINENT_PEAKS + ".gz", "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)