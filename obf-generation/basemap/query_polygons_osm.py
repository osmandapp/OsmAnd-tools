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

def process_polygons(tags, filename):
	conn_string = "host='127.0.0.1' dbname='gis' user='gisuser' password='gisuser' port='5432'"
	f = open(filename,'w')
	f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
	f.write('<osm version="0.5">\n')
 
	conn = psycopg2.connect(conn_string)
	cursor = conn.cursor()
	shift = 2
	array = ['name']
	queryFields = ", name"
	names = ['name:en', 'name:be',	'name:ca',	'name:cs',	'name:da',	'name:de',	'name:el',	
				'name:es',	'name:fi',	'name:fr',	'name:he',	'name:hi',	'name:hr',	
				'name:hu',	'name:it',	'name:ja',	'name:ko',	'name:lv',	'name:nl',	
				'name:pl',	'name:ro',	'name:ru',	'name:sk',	'name:sl',	'name:sv',	
				'name:sw',	'name:zh']
	for nm in names:
		array.append(nm)
		queryFields += ", tags->\'" + nm + "\' as \"" + nm + "\""

	conditions = " 1=0"
	admin_level = False
	for tag in tags:
		if tag == "natural" :
			array.append("wetland")
			queryFields += ", \"natural\", wetland"
			conditions += " or (\"natural\" <> '' and \"natural\" <> 'water') or wetland in ('tidalflat')"
			array.append(tag)
		elif tag == "admin_level" :
			array.append("admin_level")
			queryFields += ", admin_level"
			admin_level = True
			conditions += " or ((admin_level = '4' or admin_level = '2') and boundary <> 'national_park')"
		elif tag == "lake" :
			array.append("natural")
			array.append("seamark:type")
			array.append("seamark:restricted_area:category")
			queryFields += ", \"natural\", tags->'seamark:type' as \"seamark:type\", tags->'abandoned' as \"abandoned\", tags->'seamark:restricted_area:category' as \"seamark:restricted_area:category\""
			conditions += " or \"natural\" = 'water' or tags->'seamark:type' in ('separation_zone') or tags->'seamark:type' in ('production_area') or tags->'seamark:type' in ('restricted_area') or tags->'seamark:restricted_area:category' in ('military') or tags->'abandoned' in ('yes')"
		else :
			array.append(tag)
			queryFields += ", " + tag
			conditions += " or "+tag+" <> ''"
	sql = "select osm_id, ST_AsText(ST_Transform(ST_Simplify(way,500),94326)) " + queryFields +
	      " from planet_osm_polygon where way_area > 10000000"
	      " and ("+conditions+") "
	      # "LIMIT 1000"
	      ";"
	print "SQL : " + sql
	cursor.execute(sql)
 
	# retrieve the records from the database
	parenComma = re.compile('\)\s*,\s*\(')
	doubleParenComma = re.compile('\)\)\s*,\s*\(\(')
	trimParens = re.compile('^\s*\(?(.*?)\)?\s*$')
	rel_id = -1
	way_id = -100000000
	node_id =-10000000000000

	for row in cursor:
		if row[1] is None:
			continue
		
		tags_xml = '\t<tag k="type" v="multipolygon" />\n'
		base = shift
		while base - shift < len(array):
			if row[base] is not None:
				tags_xml += '\t<tag k="%s" v="%s" />\n' % (array[base - shift], esc(row[base]))
			base = base + 1
		# tags_xml += '\t<tag k="coordinates" v="%s" />\n' % row[1]

		polygons = []
		if row[1].startswith("POLYGON"):
			polygons = [row[1][len("POLYGON(("):-2]]
		elif row[1].startswith("MULTIPOLYGON"):
			polygons = doubleParenComma.split(row[1][len("MULTIPOLYGON((("):-3])
		else :
			polygons = [];#"#ERROR, #ERROR"

		for coordinates in polygons:
			rel_id = rel_id - 1
			xml = '\n<relation id="%s" >\n' % (rel_id)
			xml += tags_xml
			node_xml = ""
			way_xml = ""
			rings = parenComma.split(coordinates)
			first = 0
			for ring in rings:
				line = LineString(ring)
				way_id = way_id - 1;
				if first == 0:
					xml += '\t<member type="way" ref="%s" role="outer" />\n' % (way_id)
					first = 1
				else:
					xml += '\t<member type="way" ref="%s" role="inner" />\n' % (way_id)
	
				way_xml += '\n<way id="%s" >\n' % (way_id)
				if admin_level:
					way_xml += tags_xml
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
			f.write(node_xml)
			f.write(way_xml)
			if not admin_level:
				f.write(xml)
		f.write('\n')
	f.write('</osm>')

if __name__ == "__main__":
		process_polygons(['lake'], 'polygon_lake_water.osm')
		process_polygons(['landuse', 'natural', 'historic','leisure'], 'polygon_natural_landuse.osm')
		process_polygons(['aeroway', 'military', 'power', 'tourism'], 'polygon_aeroway_military_tourism.osm')
		#-1175256, -1751158 causing troubles 
		#process_polygons(['admin_level'], 'polygon_admin_level.osm') 
