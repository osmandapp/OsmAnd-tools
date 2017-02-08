def filterTags(attrs):
    if not attrs:
        return
    tags = {}

    depth = attrs['level'].split('.')[0]
    if 'depth' in attrs:
        tags['level'] = depth

    if int(depth) != 0:
	 tags.update({'contour':'depth'})

    if int(depth) != 0:
	    if int(depth) % 1000 == 0:
		tags.update({'contourtype':'1000m'})
		tags.update({'name':depth})
	    elif int(depth) % 200 == 0:
		tags.update({'contourtype':'200m'})
		tags.update({'name':depth})

    return tags