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

def process_roads(cond, filename, fields):
	conn_string = "host='127.0.0.1' dbname='gis' user='gisuser' password='gisuser' port='5432'"
	f = open(filename,'w')
	f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
	f.write('<osm version="0.5">\n')
 
	conn = psycopg2.connect(conn_string)
	cursor = conn.cursor()
	array = ['name', 'ref', 'int_ref']
	names = ['name:en', 'name:be',	'name:ca',	'name:cs',	'name:da',	'name:de',	'name:el',	
				'name:es',	'name:fi',	'name:fr',	'name:he',	'name:hi',	'name:hr',	
				'name:hu',	'name:it',	'name:ja',	'name:ko',	'name:lv',	'name:nl',	
				'name:pl',	'name:ro',	'name:ru',	'name:sk',	'name:sl',	'name:sv',	
				'name:sw',	'name:zh']
	selectFields = ""
	for nm in names:
		array.append(nm)
		selectFields += ", tags->\'" + nm + "\' as \"" + nm + "\""
	
	for field in fields:
		if field = 'seamark:type':
			field = "tags->'seamark:type' as 'seamark:type'"
		array.append(field)
		selectFields += ", " + field	
	shift = 2
	# roads faster but doesn't contain ferry & river
	sql = "select osm_id, ST_AsText(ST_Transform(ST_Simplify(way,50),94326))," + \
	      " name, ref, tags->'int_ref' as int_ref " + selectFields + \
	      " from planet_osm_line where " + cond + ";"
	      # "LIMIT 1000"
	#print sql
	cursor.execute(sql)
 
	node_id =-1000000000
	way_id = 1
	for row in cursor:
		node_xml = ""
		way_xml = ""
		way_id = way_id + 1
		way_xml = '\n<way id="%s" >\n' % (row[0] + way_id * 10000000000)		
		base = shift
		while base - shift < len(array):
			if row[base] is not None:
				way_xml += '\t<tag k="%s" v="%s" />\n' % (array[base - shift], esc(row[base]))
			base = base + 1
		if not row[1].startswith("LINESTRING("):
			raise Exception("Object " + row[0] + " has bad geometry" + row[1])
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
	
	
	#process_roads("highway='motorway'", "line_motorway.osm", ['highway', 'junction', 'route'])
	#process_roads("highway='trunk'", "line_trunk.osm", ['highway', 'junction', 'route'])
	#process_roads("highway='primary'", "line_primary.osm", ['highway', 'junction', 'route'])
	#process_roads("highway='secondary'", "line_secondary.osm", ['highway', 'junction', 'route'])
	#process_roads("railway='rail'", "line_railway.osm", ['railway'])
	#process_roads("highway='tertiary'", "line_tertiary.osm", ['highway', 'junction', 'route'])
	process_roads("route='ferry' or (tags->'seamark:type' in ('separation_line', 'separation_lane', 'separation_boundary', 'light_major'))", "line_ferry.osm", ['route', 'seamark:type'])
	
	# not used
	#process_roads("(admin_level = '4' or admin_level = '2')", "line_admin_level.osm", ['admin_level'])
	#process_roads("waterway='river' or waterway='canal' ", "line_rivers.osm", ['waterway'])
