def filterTags(attrs):
	if not attrs or 'depth' not in attrs:
		return
	tags = {}

	depth = int(float(attrs['depth']))
	if depth == 0:
		return tags
	tags['depth'] = str(depth)
	tags['contour'] = 'depth'
	tags['name'] = str(depth)

	if depth % 1000 == 0:
		tags['contourtype'] = '1000m'
	elif depth % 200 == 0:
		tags['contourtype'] = '200m'
	elif depth % 100 == 0:
		tags['contourtype'] = '100m'
	elif depth % 50 == 0:
		tags['contourtype'] = '50m'
	elif depth % 20 == 0:
		tags['contourtype'] = '20m'
	elif depth % 10 == 0:
		tags['contourtype'] = '10m'
	elif depth % 5 == 0:
		tags['contourtype'] = '5m'
	elif depth == 2:
		tags['contourtype'] = '2m'
	else:
		# 1 m steps of a charted source (Kartverket has them in harbours): minor contours, from zoom 13 only
		tags['contourtype'] = '1m'
	return tags
