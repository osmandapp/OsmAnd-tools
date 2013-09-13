#!/usr/bin/python
import psycopg2
import sys
import pprint
import re

def selectMapping(row):
	a = dict()
	base = 3;
	array = ['landuse', 'natural', 'historic','aeroway','leisure','man_made','military','power','tourism',
			'water','waterway']
	while base - 3 < len(array):
		if row[base] is not None:
			a[array[base-3]]=row[base]
		base=base+1
	return a

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

def main():
	conn_string = "host='127.0.0.1' dbname='osm' user='osm' password='osm' port='5433'"
	print '<?xml version="1.0" encoding="UTF-8"?>'
	print '<osm version="0.5">'
 
	# get a connection, if a connect cannot be made an exception will be raised here
	conn = psycopg2.connect(conn_string)
 
	# conn.cursor will return a cursor object, you can use this not cursor to perform queries
	cursor = conn.cursor()
 
	# execute our Query
	cursor.execute("select name, osm_id, ST_AsText(ST_Transform(ST_Simplify(way,100),4326)), landuse, \"natural\", historic, aeroway, "
				   "    leisure, man_made, military, power, tourism, water, waterway "
				   " from planet_osm_polygon where way_area > 1000000"
				   " and (landuse <> '' or \"natural\" <> '' or aeroway <> '' or historic <> '' or leisure <> '' or man_made <> ''"
				   " or military <> '' or  power <> '' or tourism <> '' or water <> '' or waterway <> '' ) "
				  # "LIMIT 1000"
				   ";")
 
	# retrieve the records from the database
	row_count = 0
	parenComma = re.compile('\)\s*,\s*\(')
	trimParens = re.compile('^\s*\(?(.*?)\)?\s*$')
	rel_id = -1
	way_id = -100000000
	node_id =-10000000000000

	for row in cursor:
		if row[2] is None:
			continue
		row_count += 1
		mapping = selectMapping(row)
		node_xml = ""
		way_xml = ""
		rel_id = rel_id - 1
		xml = '\n<relation id="%s" >\n' % (rel_id)
		xml += '\t<tag k="type" v="multipolygon" />\n'
		if row[0] is not None:
			xml += '\t<tag k="name" v="%s" />\n' % (esc(row[0]))
		for key, value in mapping.items():
			xml += '\t<tag k="%s" v="%s" />\n' % (key, esc(value))
		coordinates = row[2][len("POLYGON("):-1]
		rings = parenComma.split(coordinates)
		first = 0
		for i,ring in enumerate(rings):
			ring = trimParens.match(ring).groups()[0]
			line = LineString(ring)
			way_id = way_id - 1;
			if first == 0:
				xml += '\t<member type="way" ref="%s" role="outer" />\n' % (way_id)
				first = 1
			else:
				xml += '\t<member type="way" ref="%s" role="inner" />\n' % (way_id)

			way_xml += '\n<way id="%s" >\n' % (way_id)
			first_node_id = 0
			first_node = []
			for c in line:
				node_id = node_id - 1
				nid = node_id
				if first_node_id == 0:
					first_node_id = node_id
					first_node = c
					node_xml += '\n<node id="%s" lat="%s" lon="%s"/>' % (nid, c[1], c[0])
				elif first_node == c:
					nid = first_node_id
				else:
					node_xml += '\n<node id="%s" lat="%s" lon="%s"/>' % (nid, c[1], c[0])
				way_xml += '\t<nd ref="%s" />\n' % (nid)
			way_xml += '</way>'
		xml += '</relation>'	
		print "%s %s %s \n" % ( node_xml, way_xml, xml)
	print '</osm>'

if __name__ == "__main__":
	main()