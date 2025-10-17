#!/usr/bin/python
import psycopg2
import sys
import pprint
import re
import os

def num(s, df):
	if s is None:
		return df
	try:
		return int(s)
	except ValueError:
		try:
			return float(s)
		except ValueError:
			return df

def esc(s):
	return s.replace("&", "&amp;").replace("\"", "&quot;").replace("<", "&lt;").replace(">", "&gt;").replace("'","&apos;")

def process_points(cond, filename, array):
	f = open(filename,'w')
	conn_string = "host='127.0.0.1' dbname='"+os.environ['DB_NAME']+"' user='"+os.environ['DB_USER']+"' password='"+os.environ['DB_PWD']+"' port='5432'"
	f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
	f.write('<osm version="0.6">\n')
	conn = psycopg2.connect(conn_string)
	cursor = conn.cursor()
	shift = 2
	queryFields = ""
	names = ['name:af', 'name:ar', 'name:az', 'name:be', 'name:bg', 'name:bn', 'name:bpy', 'name:br', 'name:bs', 'name:ca', 'name:ceb', 'name:cs', 'name:cy', 'name:da', 'name:de', 'name:el', 'name:eo', 'name:es', 'name:et', 'name:eu', 'name:id', 'name:fa', 'name:fi', 'name:fr', 'name:fy', 'name:ga', 'name:gl', 'name:he', 'name:hi', 'name:hr', 'name:ht', 'name:hu', 'name:hy', 'name:is', 'name:it', 'name:ja', 'name:ka', 'name:kn', 'name:ko', 'name:ku', 'name:la', 'name:lb', 'name:lt', 'name:lv', 'name:mk', 'name:ml', 'name:mr', 'name:ms', 'name:nds', 'name:new', 'name:nl', 'name:nn', 'name:no', 'name:nv', 'name:os', 'name:pl', 'name:pms', 'name:pt', 'name:ro', 'name:ru', 'name:sc', 'name:sh', 'name:sk', 'name:sl', 'name:sq', 'name:sr', 'name:sv', 'name:sw', 'name:ta', 'name:te', 'name:th', 'name:tl', 'name:tr', 'name:uk', 'name:vi', 'name:vo', 'name:zh']
	largeStatesList = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming', 'Alberta', 'British Columbia', 'Manitoba', 'New Brunswick', 'Newfoundland and Labrador', 'Northwest Territories', 'Nova Scotia', 'ᓄᓇᕗᑦ Nunavut', 'Ontario', 'Québec', 'Saskatchewan', 'Yukon', 'Western Australia', 'Northern Territory', 'Queensland', 'South Australia', 'New South Wales', 'Victoria', 'Tasmania']
	for tag in array:
		if tag == 'name:en':
			tag = 'tags->\'name:en\' as "name:en"'
		if tag == 'iata':
			tag = 'tags->\'iata\' as "iata"'
		if tag == 'icao':
			tag = 'tags->\'icao\' as "icao"'
		if tag == 'faa':
			tag = 'tags->\'faa\' as "faa"'
		if tag == 'natural':
			tag = '"natural"'
		if tag == 'seamark:type':
			tag = 'tags->\'seamark:type\' as "seamark:type"'
		if tag == 'abandoned':
			tag = 'tags->\'abandoned\' as "abandoned"'
		if tag == 'population':
			tag = 'tags->\'population\' as "population"'
		if tag == 'capital':
			tag = 'tags->\'capital\' as "capital"'
		if tag == 'ele':
			tag = 'tags->\'ele\' as "ele"'
		if tag == 'wikidata':
			tag = 'tags->\'wikidata\' as "wikidata"'
		queryFields += ", " + tag

	for nm in names:
		array.append(nm)
		queryFields += ", tags->\'" + nm + "\' as \"" + nm + "\""
		
	sql = "select ST_AsText(ST_Transform(way,4326)), osm_id  " + queryFields + \
	      " from planet_osm_point where " + cond + ";"
	      # "LIMIT 2"
	print(sql)
	cursor.execute(sql)
 	
	node_id =-1000
	parse = re.compile(r'(-?[\d.]+)\s(-?[\d.]+)')
	for row in cursor:
		checkForLargeState = False
		node_id = row[1] #node_id - 1
		match = parse.search(row[0])
		xml = '\n<node version="1" id="%s" lat="%s" lon="%s">\n' % (node_id, match.groups()[1], match.groups()[0])
		base = shift
		while base - shift < len(array):
			if row[base] is not None:
				tagName = array[base - shift]
				value = esc(row[base])
				if tagName == "name" and value in largeStatesList:
					checkForLargeState = True;
				if tagName == "place" and value == "state" and checkForLargeState:
					xml += '\t<tag k="osmand_large_state" v="true" />\n'
				if tagName == "place" and value == "city" :
					pop = num(row[2], 0)
					if pop > 100000	:
						xml += '\t<tag k="%s" v="%s" />\n' % ("osmand_place_basemap", "city")
				# if tagName == 'name' and row[base+1] is not None and len(row[base+1]) > 0 :
					# value = '' # write name:en instead of name
				# if tagName == 'name:en' and len(value) > 0 :
					# tagName = 'name'
				
				if len(value) > 0 :
					if not (tagName == "ref" and len(value) > 6) :
						xml += '\t<tag k="%s" v="%s" />\n' % (tagName, value)
			base = base + 1
		xml += '</node>'
		f.write(xml)
	f.write('</osm>')

if __name__ == "__main__":
	print("Process points")
	process_points("place in ('continent','sea','ocean','state','country') or \"natural\" = 'strait'", 'points_main.osm',
			['name', 'name:en', 'place', 'ref', 'population', 'natural', 'wikidata'])
	process_points("place in ('city','town') ", 'cities.osm', ['name', 'name:en', 'place', 'capital', 'population', 'wikidata'])
	process_points("place in ('city') ", 'points_only_cities.osm', ['name', 'name:en', 'place', 'capital', 'population', 'wikidata'])
#	process_points("place in ('county') "
#				   " or \"natural\" in ('peak', 'cave_entrance', 'rock', 'waterfall', 'cape', 'volcano', 'stream', 'reef')"
#				   " or tourism in ('alpine_hut') "
#				   " or tags->'seamark:type' in ('light_major') "
#				   " or tags->'seamark:type' in ('harbour') "
#				   " or tags->'abandoned' in ('yes') "
#				   " or (tags->'population' <> '')"
#				   " or aeroway in ('aerodrome', 'airport')"
#		       		   , 'points_less.osm', 
#				   ['name', 'name:en',
#				    'ref', 'ele', 'place','natural', 'seamark:type', 'abandoned', 'aeroway', 'tourism', 'iata', 'icao', 'faa', 'population'])
