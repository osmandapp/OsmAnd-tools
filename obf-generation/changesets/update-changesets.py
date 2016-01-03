# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from datetime import time, tzinfo
import urllib2
import xmltodict
import os
import re
import gzip
import psycopg2

# DDL
# CREATE TABLE pending_changesets (id text, created_at text);
# CREATE TABLE changesets (id text, bot int, created_at timestamp, closed_at timestamp, username text, closed_at_day text, uid text);
# GRANT ALL privileges ON ALL TABLES IN SCHEMA public to ;
max_query_changeset = 150
begin_query = 36347200
conn_string = "host='localhost' dbname='changeset' user='"+os.environ['DB_USER']+"' password='"+os.environ['DB_PWD']+"'"
conn = psycopg2.connect(conn_string)
c = conn.cursor()
line = ""
values = 0
start = begin_query
c.execute("SELECT id from pending_changesets")
res = c.fetchall()
if res is not None:
	for row in res:
		print 'Pending ' + str(row[0])
		if values > 0:
			line = line + ','
		line = line + str(row[0]);
		values = values + 1;
		start = max(start, int(row[0]))


if values < max_query_changeset:
	c.execute("SELECT max(id) from changesets")
	res = c.fetchall()
	if res is not None and len(res) > 0 and res[0][0] is not None:
		start = max(int(res[0][0]), start);
	while values < max_query_changeset:
		if values > 0:
			line = line + ','
		line = line + str(start);	
		values = values + 1;
		c.execute("INSERT INTO pending_changesets VALUES (%s, %s)", (str(start), ''))
		start = start + 1
conn.commit()


file = urllib2.urlopen('http://api.openstreetmap.org//api/0.6/changesets?changesets='+line)
print "query http://api.openstreetmap.org//api/0.6/changesets?changesets="+line
data = file.read()
file.close()
data = xmltodict.parse(data)
maxdate = None
for i, (key, value) in enumerate(data['osm'].iteritems()):
	if key == 'changeset':
		for vl in value:
			#if '@bot'
			if '@closed_at' in vl:
				c.execute("DELETE FROM pending_changesets where id = %s", (vl['@id'], ))
				c.execute("INSERT INTO changesets(id, bot,created_at,closed_at,closed_at_day,username,uid)" +
				                        " VALUES (%s, %s, %s, %s, %s, %s, %s)", 
				                        (vl['@id'],0,vl['@created_at'].replace('T', ' '),
				                         vl['@closed_at'].replace('T', ' '),vl['@closed_at'][0:10],vl['@user'],vl['@uid']))
				v =  u' - '.join([vl['@id'], vl['@user'], vl['@closed_at']])
				if maxdate is None:
					maxdate = vl['@closed_at']
				else:
					maxdate = max(maxdate, vl['@closed_at'])
				print v;
conn.commit()
print 'Max date ' + maxdate


# OLD CODE
# line = ""
# with open('last_status') as f:
    # line = f.readlines()[0]
# print 'Last status ' + line
#tm = time.strptime('%Y-%m-%dT%H:%M:%S', line)
#2015-12-31T04:05:06
#print time.strftime('%Y-%m-%dT%H:%M:%S', now())
