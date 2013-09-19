#!/usr/bin/python
import psycopg2
import sys
import pprint
import re

def esc(s):
	return s.replace("\"", "&quot;").replace("<", "&lt;").replace(">", "&gt;").replace("&", "&amp;").replace("'","&apos;")

def process_points(filename):
	f = open(filename,'w')
	conn_string = "host='127.0.0.1' dbname='gis' user='gisuser' password='gisuser' port='5433'"
	f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
	f.write('<osm version="0.5">\n')
 
	conn = psycopg2.connect(conn_string)
	cursor = conn.cursor()
	shift = 2
	array = ['name', 'ref', 'ele', 'place','natural', 'aeroway', 'tourism']
	cursor.execute("select ST_AsText(ST_Transform(way,4326)), osm_id, name, ref, ele, place, \"natural\", aeroway, tourism"
				   " from planet_osm_point where place in ('sea','ocean','state', 'country') "
				   " or \"natural\" in ('peak', 'cave_entrance', 'rock', 'waterfall', 'cape', 'volcano', 'stream')"
				   " or tourism in ('alpine_hut') "
				   " or aeroway in ('aerodrome', 'airport')"
				   # "LIMIT 2"
				   ";")
 
	node_id =-1000
	parse = re.compile('(-?[\d|\.]+)\s(-?[\d|\.]+)')
	for row in cursor:
		node_id = row[1] #node_id - 1
		match = parse.search(row[0])
		xml = '\n<node id="%s" lat="%s" lon="%s">\n' % (node_id, match.groups()[1], match.groups()[0])
		base = shift
		while base - shift < len(array):
			if row[base] is not None:
				xml += '\t<tag k="%s" v="%s" />\n' % (array[base - shift], esc(row[base]))
			base = base + 1
		xml += '</node>'	
		f.write(xml)
	f.write('</osm>')

if __name__ == "__main__":
	process_points('points.osm')