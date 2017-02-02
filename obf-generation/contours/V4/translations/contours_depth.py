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
	    if int(depth) % 100 == 0:
		tags.update({'contourtype':'100m'})
		tags.update({'name':depth})
	    elif int(depth) % 50 == 0:
		tags.update({'contourtype':'50m'})
		tags.update({'name':depth})
	    elif int(depth) % 20 == 0:
		tags.update({'contourtype':'20m'})
		tags.update({'name':depth})
	    elif int(depth) % 10 == 0:
		tags.update({'contourtype':'10m'})
		tags.update({'name':depth})

    return tags