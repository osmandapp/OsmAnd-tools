#!/usr/bin/python
import psycopg2
import sys
import pprint
import re
import os

regSpaces = re.compile(r'\s+')
def Point(geoStr):
	coords = regSpaces.split(geoStr.strip())
	return [coords[0],coords[1]]

def LineString(geoStr):
	points = geoStr.strip().split(',')
	points = map(Point,points)
	return points

def esc(s):
	return s.replace("&", "&amp;").replace("\"", "&quot;").replace("<", "&lt;").replace(">", "&gt;").replace("'","&apos;")

def process_polygons(tags, filename):
	conn_string = "host='127.0.0.1' dbname='"+os.environ['DB_NAME']+"' user='"+os.environ['DB_USER']+"' password='"+os.environ['DB_PWD']+"' port='5432'"
	f = open(filename,'w')
	f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
	f.write('<osm version="0.6">\n')
 
	conn = psycopg2.connect(conn_string)
	cursor = conn.cursor()
	shift = 2
	array = ['name']
	queryFields = ", name"
	names = ['name:af', 'name:ar', 'name:az', 'name:be', 'name:bg', 'name:bn', 'name:bpy', 'name:br', 'name:bs', 'name:ca', 'name:ceb', 'name:crh', 'name:cs', 'name:cy', 'name:da', 'name:de', 'name:el', 'name:en', 'name:eo', 'name:es', 'name:et', 'name:eu', 'name:id', 'name:fa', 'name:fi', 'name:fr', 'name:fy', 'name:ga', 'name:gl', 'name:he', 'name:hi', 'name:hr', 'name:ht', 'name:hu', 'name:hy', 'name:is', 'name:it', 'name:ja', 'name:ka', 'name:kn', 'name:ko', 'name:ku', 'name:la', 'name:lb', 'name:lt', 'name:lv', 'name:mk', 'name:ml', 'name:mr', 'name:ms', 'name:nds', 'name:new', 'name:nl', 'name:nn', 'name:no', 'name:nv', 'name:os', 'name:pl', 'name:pms', 'name:pt', 'name:ro', 'name:ru', 'name:sc', 'name:sh', 'name:sk', 'name:sl', 'name:sq', 'name:sr', 'name:sv', 'name:sw', 'name:ta', 'name:te', 'name:th', 'name:tl', 'name:tr', 'name:uk', 'name:vi', 'name:vo', 'name:zh']
	for nm in names:
		array.append(nm)
		queryFields += ", tags->\'" + nm + "\' as \"" + nm + "\""

	array.append("wikidata")
	queryFields += ", tags->'wikidata' as \"wikidata\""

	conditions = " 1=0"
	admin_level = False
	for tag in tags:
		if tag == "natural" :
			array.append("natural")
			queryFields += ", \"natural\""
			conditions += " or (\"natural\" <> '' and \"natural\" <> 'water') and \"natural\" <> 'bare_rock' and \"natural\" <> 'rock' and \"natural\" <> 'stone' and \"natural\" <> 'sand' and \"natural\" <> 'cave_entrance' and \"natural\" <> 'scree' and \"natural\" <> 'fell' and \"natural\" <> 'scrub' and \"natural\" <> 'heath' and \"natural\" <> 'grassland' and \"natural\" <> 'coastline'"
		elif tag == "wetland" :
			array.append("wetland")
			queryFields += ", tags->'wetland' as \"wetland\""
			conditions += " or tags->'wetland' <> ''"
		elif tag == "landuse" :
			array.append("landuse")
			queryFields += ", \"landuse\""
			conditions += " or (\"landuse\" = 'residential' or \"landuse\" = 'allotments' or \"landuse\" = 'industrial' or \"landuse\" = 'forest' or \"landuse\" = 'military')"
		elif tag == "leisure" :
			array.append("leisure")
			queryFields += ", \"leisure\""
			conditions += " or (\"leisure\" = 'nature_reserve' or \"leisure\" = 'ski_resort')"
		elif tag == "admin_level_2" : 
			array.append("admin_level")
			queryFields += ", admin_level"
			admin_level = True
			conditions += " or (admin_level = '2' and boundary <> 'national_park')"	     
		elif tag == "admin_level_4" :
			array.append("admin_level")
			queryFields += ", admin_level"
			admin_level = True
			conditions += " or (admin_level = '4' and boundary <> 'national_park')"
		elif tag == "seamark:type" :
			array.append("seamark:type")
			queryFields += ",  tags->'seamark:type' as \"seamark:type\""
			conditions += " or tags->'seamark:type' in ('separation_zone') or tags->'seamark:type' in ('production_area') or tags->'seamark:type' in ('restricted_area') or tags->'seamark:type' in ('sea_area')"
		elif tag == "seamark:restricted_area:category" :
			array.append("seamark:restricted_area:category")
			queryFields += ", tags->'seamark:restricted_area:category' as \"seamark:restricted_area:category\""
			conditions += " or tags->'seamark:restricted_area:category' in ('military')"
		elif tag == "lake" :
			array.append("natural")
			queryFields += ", \"natural\""
			conditions += " or \"natural\" = 'water' "
		elif tag == "abandoned" :
			array.append("abandoned")
			queryFields += ", tags->'abandoned' as \"abandoned\""
			conditions += " or tags->'abandoned' in ('yes')"
		elif tag == "iata" :
			array.append("iata")
			queryFields += ", tags->'iata' as \"iata\""
			conditions += " or tags->'iata' <> ''"
		elif tag == "icao" :
			array.append("icao")
			queryFields += ", tags->'icao' as \"icao\""
			conditions += " or tags->'icao' <> ''"
		elif tag == "faa" :
			array.append("faa")
			queryFields += ", tags->'faa' as \"faa\""
			conditions += " or tags->'faa' <> ''"
		else :
			array.append(tag)
			queryFields += ", " + tag
			conditions += " or "+tag+" <> ''"
	sql =( "select osm_id, ST_AsText(ST_Transform(ST_SimplifyPreserveTopology(way, 500), 4326)) " + queryFields +
	       " from planet_osm_polygon where way_area > 10000000"+
	       " and ("+conditions+") "+
	       # "LIMIT 1000"
	       ";" )
	print("SQL : " + sql)
	cursor.execute(sql)
 
	# retrieve the records from the database
	parenComma = re.compile(r'\)\s*,\s*\(')
	doubleParenComma = re.compile(r'\)\)\s*,\s*\(\(')
	trimParens = re.compile(r'^\s*\(?(.*?)\)?\s*$')
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
				if len(ring) > 0:
					line = LineString(ring)
					way_id = way_id - 1;
					if first == 0:
						xml += '\t<member type="way" ref="%s" role="outer" />\n' % (way_id)
						first = 1
					else:
						xml += '\t<member type="way" ref="%s" role="inner" />\n' % (way_id)

					way_xml += '\n<way version="1" id="%s" >\n' % (way_id)
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
				else:
					print("Empty ring!")
			xml += '</relation>'	
			f.write(node_xml)
			f.write(way_xml)
			if not admin_level:
				f.write(xml)
		f.write('\n')
	f.write('</osm>')

if __name__ == "__main__":
		print("Process polygons")
		process_polygons(['lake', 'seamark:type', 'seamark:restricted_area:category'], 'polygon_lake_water.osm')
		process_polygons(['landuse', 'natural', 'wetland', 'historic', 'leisure'], 'polygon_natural_landuse.osm')
		process_polygons(['aeroway', 'military', 'abandoned', 'iata', 'icao', 'faa', 'power', 'tourism'], 'polygon_aeroway_military_tourism.osm')
		#-1175256, -1751158 causing troubles 
		process_polygons(['admin_level_2'], 'polygon_admin_level_2.osm') 
		process_polygons(['admin_level_4'], 'polygon_admin_level_4.osm')
