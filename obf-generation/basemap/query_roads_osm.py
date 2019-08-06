#!/usr/bin/python
import psycopg2
import sys
import pprint
import re
import os

regSpaces = re.compile('\s+')
def Point(geoStr):
	coords = regSpaces.split(geoStr.strip())
	return [coords[0],coords[1]]

def LineString(geoStr):
	points = geoStr.strip().split(',')
	points = map(Point,points)
	return points

def esc(s):
	return s.replace("&", "&amp;").replace("\"", "&quot;").replace("<", "&lt;").replace(">", "&gt;").replace("'","&apos;")

def process_roads(cond, filename, fields):
	print "Query %s" % cond
	conn_string = "host='127.0.0.1' dbname='osm' user='"+os.environ['DB_USER']+"' password='"+os.environ['DB_PWD']+"' port='5432'"
	f = open(filename,'w')
	f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
	f.write('<osm version="0.6">\n')
 
	conn = psycopg2.connect(conn_string)
	cursor = conn.cursor()
	array = ['name', 'ref', 'int_ref']
	names = ['name:af', 'name:ar', 'name:az', 'name:be', 'name:bg', 'name:bn', 'name:bpy', 'name:br', 'name:bs', 'name:ca', 'name:ceb', 'name:cs', 'name:cy', 'name:da', 'name:de', 'name:el', 'name:en', 'name:eo', 'name:es', 'name:et', 'name:eu', 'name:id', 'name:fa', 'name:fi', 'name:fr', 'name:fy', 'name:ga', 'name:gl', 'name:he', 'name:hi', 'name:hr', 'name:ht', 'name:hu', 'name:hy', 'name:is', 'name:it', 'name:ja', 'name:ka', 'name:kn', 'name:ko', 'name:ku', 'name:la', 'name:lb', 'name:lt', 'name:lv', 'name:mk', 'name:ml', 'name:mr', 'name:ms', 'name:nds', 'name:new', 'name:nl', 'name:nn', 'name:no', 'name:nv', 'name:os', 'name:pl', 'name:pms', 'name:pt', 'name:ro', 'name:ru', 'name:sc', 'name:sh', 'name:sk', 'name:sl', 'name:sq', 'name:sr', 'name:sv', 'name:sw', 'name:ta', 'name:te', 'name:th', 'name:tl', 'name:tr', 'name:uk', 'name:vi', 'name:vo', 'name:zh']
	selectFields = ""
	for nm in names:
		array.append(nm)
		selectFields += ", tags->\'" + nm + "\' as \"" + nm + "\""
	
	for field in fields:
		array.append(field)
		if field == 'seamark:type':
			field = "tags->'seamark:type' as \"seamark:type\""
		selectFields += ", " + field	
	shift = 2
	# roads faster but doesn't contain ferry & river
	sql = "select osm_id, ST_AsText(ST_Transform(ST_Simplify(way,50,true),94326))," + \
	      " name, ref, tags->'int_ref' as int_ref " + selectFields + \
	      " from planet_osm_line where (" + cond + ") and osm_id > 0 order by osm_id asc, pg_column_size(way) desc;"
	      # "LIMIT 1000"
	#print sql
	cursor.execute(sql)
 
	node_id =-10000000000
	max_way_id_start = 1000000000
	wd_id = max_way_id_start
	way_id = 0
	extra_way_xml = ""
	for row in cursor:
		if row[1] is None:
			continue;
		node_xml = ""

		if way_id == row[0]:
			print "Warning duplicate road id %s in db" % row[0]
			wd_id = wd_id + 1
			way_xml = '\n<way version="1" id="%s" >\n' % (wd_id)
		else:
			way_id = row[0]
			way_xml = '\n<way version="1" id="%s" >\n' % (way_id)
		base = shift
		while base - shift < len(array):
			if row[base] is not None:
				way_xml += '\t<tag k="%s" v="%s" />\n' % (array[base - shift], esc(row[base]))
			base = base + 1

		if not row[1].startswith("LINESTRING("):
			raise Exception("Object " + row[0] + " has bad geometry" + row[1])
		coordinates = LineString(row[1][len("LINESTRING("):-1])
		for c in coordinates :
			node_id = node_id + 1
			nid = node_id
			node_xml += '\n<node id="%s" lat="%s" lon="%s"/>' % (nid, c[1], c[0])
			way_xml += '\t<nd ref="%s" />\n' % (nid)
		f.write(node_xml)
		way_xml += '</way>\n'
		if way_id > max_way_id_start:
			extra_way_xml += way_xml
		else:
			f.write(way_xml)

	# if way_id != 0:
	# 	way_xml += '</way>'
	f.write(extra_way_xml)
	f.write('</osm>')

if __name__ == "__main__":
	process_roads("highway='primary' or highway='primary_link'", "line_primary.osm", ['highway', 'junction', 'route'])	
	process_roads("highway='motorway' or highway='motorway_link'", "line_motorway.osm", ['highway', 'junction', 'route'])
	process_roads("highway='trunk' or highway='trunk_link'", "line_trunk.osm", ['highway', 'junction', 'route'])
	process_roads("highway='secondary' or highway='secondary_link'", "line_secondary.osm", ['highway', 'junction', 'route'])
	process_roads("railway='rail'", "line_railway.osm", ['railway'])
	process_roads("highway='tertiary' or highway='tertiary_link'", "line_tertiary.osm", ['highway', 'junction', 'route'])
	process_roads("route='ferry' or (tags->'seamark:type' in ('separation_line', 'separation_lane', 'separation_boundary'))", "line_ferry.osm", ['route', 'seamark:type'])
	process_roads("admin_level='4'", "line_admin_level_4.osm", ['admin_level'])
	process_roads("admin_level='2'", "line_admin_level_2.osm", ['admin_level'])
	
	# not used
	#	process_roads("(admin_level = '4' or admin_level = '2')", "line_admin_level.osm", ['admin_level'])
	#process_roads("waterway='river' or waterway='canal' ", "line_rivers.osm", ['waterway'])
