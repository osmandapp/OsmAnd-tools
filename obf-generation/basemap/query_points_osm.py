#!/usr/bin/python
import psycopg2
import sys
import pprint
import re

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
	cursor.execute("select ST_AsText(ST_Transform(way,4326)), name, ref, ele, place, \"natural\""
				   " from planet_osm_point where place in ('sea','ocean','state', 'country') "
				   " or \"natural\" in ('peak', 'cave_entrance', 'rock', 'waterfall', 'cape', 'volcano')"
				   # "LIMIT 2"
				   ";")
 
	# retrieve the records from the database
	row_count = 0
	node_id =-1000
	parse = re.compile('(-?[\d|\.]+)\s(-?[\d|\.]+)')
	for row in cursor:
		row_count += 1
		node_id = node_id - 1
		match = parse.search(row[0])
		xml = '\n<node id="%s" lat="%s" lon="%s">\n' % (node_id, match.groups()[0], match.groups()[1])
		array = ['name', 'ref', 'ele','place','natural']
		base = 1
		while base - 1 < len(array):
			if row[base] is not None:
				xml += '\t<tag k="%s" v="%s" />\n' % (array[base - 1], esc(row[base]))
			base = base + 1
		xml += '</node>'	
		print xml
	print '</osm>'

if __name__ == "__main__":
	main()