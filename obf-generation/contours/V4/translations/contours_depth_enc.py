def filterTags(attrs):
	if not attrs:
		return
	tags = {}
	
	depth = str(round(float(attrs['depth']),1))
	if depth.endswith('.0'):
		depth = depth[:-2]
	if 'depth' in attrs:
		tags['depth'] = depth

	if depth != 0:
		tags.update({'contour':'depth'})

	if depth != 0:
		if float(depth) % 1000 == 0 and int(depth) != 0:
			tags.update({'contourtype':'1000m'})
			tags.update({'name':depth})
		elif float(depth) % 200 == 0 and int(depth) != 0:
			tags.update({'contourtype':'200m'})
			tags.update({'name':depth})
		elif float(depth) % 100 == 0 and int(depth) != 0:
			tags.update({'contourtype':'100m'})
			tags.update({'name':depth})
		elif float(depth) % 50 == 0 and int(depth) != 0:
			tags.update({'contourtype':'50m'})
			tags.update({'name':depth})
		elif float(depth) % 20 == 0 and int(depth) != 0:
			tags.update({'contourtype':'20m'})
			tags.update({'name':depth})
		elif float(depth) % 10 == 0 and int(depth) != 0:
			tags.update({'contourtype':'10m'})
			tags.update({'name':depth})
		elif float(depth) % 5 == 0 and int(depth) != 0:
			tags.update({'contourtype':'5m'})
			tags.update({'name':depth})

	if depth != 0:
		if float(depth) == 1 :
			tags.update({'safetycontour':'1m'})
		if float(depth) == 2 :
			tags.update({'safetycontour':'2m'})
		if float(depth) == 3 :
			tags.update({'safetycontour':'3m'})
		if float(depth) == 5 :
			tags.update({'safetycontour':'5m'})
		if float(depth) == 7 :
			tags.update({'safetycontour':'7m'})
		if float(depth) == 10 :
			tags.update({'safetycontour':'10m'})

	return tags