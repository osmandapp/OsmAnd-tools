#!/usr/bin/python
import psycopg2
import sys
import pprint
import re

regSpaces = re.compile('\s+')
def Point(geoStr):
	coords = regSpaces.split(geoStr.strip())
	return [coords[0],coords[1]]

def LineString(geoStr):
	points = geoStr.strip().split(',')
	points = map(Point,points)
	return points

def esc(s):
	return s.replace("\"", "&quot;").replace("<", "&lt;").replace(">", "&gt;").replace("&", "&amp;").replace("'","&apos;")

def process_roads(cond, filename):
	conn_string = "host='127.0.0.1' dbname='osm' user='osm' password='osm' port='5433'"
	f = open(filename,'w')
	f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
	f.write('<osm version="0.5">\n')
 
	conn = psycopg2.connect(conn_string)
	cursor = conn.cursor()
	array = ['name', 'ref', 'highway','railway','waterway', 'junction']
	shift = 2
	cursor.execute("select osm_id, ST_AsText(ST_Transform(ST_Simplify(way,50),4326)),"
				   " name, ref, highway, railway, waterway, junction "
				   " from planet_osm_line where " + cond + # ST_Length(way) > 100 and
				  # "LIMIT 1000"
				   ";")
 
	node_id =-1000000000
	for row in cursor:
		node_xml = ""
		way_xml = ""
		way_xml = '\n<way id="%s" >\n' % (row[0])		
		base = shift
		while base - shift < len(array):
			if row[base] is not None:
				way_xml += '\t<tag k="%s" v="%s" />\n' % (array[base - shift], esc(row[base]))
			base = base + 1
		coordinates = LineString(row[1][len("LINESTRING("):-1])

		for c in coordinates :
			node_id = node_id - 1
			nid = node_id
			node_xml += '\n<node id="%s" lat="%s" lon="%s"/>' % (nid, c[1], c[0])
			way_xml += '\t<nd ref="%s" />\n' % (nid)
		way_xml += '</way>'
		f.write(node_xml)
		f.write(way_xml)
		f.write('\n')
	f.write('</osm>')

if __name__ == "__main__":
	process_roads("highway='motorway'", "line_motorway.osm")
	process_roads("highway='trunk'", "line_trunk.osm")
	process_roads("highway='primary'", "line_primary.osm")
	process_roads("highway='secondary'", "line_secondary.osm")
	process_roads("highway='tertiary'", "line_tertiary.osm")
	process_roads("railway='rail'", "line_railway.osm")
	#process_roads("waterway='river' or waterway='canal' ", "line_rivers.osm")