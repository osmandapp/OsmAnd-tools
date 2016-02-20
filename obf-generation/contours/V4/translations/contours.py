def filterTags(attrs):
    if not attrs:
        return
    tags = {}

    height = attrs['height'].split('.')[0]
    if 'height' in attrs:
        tags['elevation'] = height

    if int(height) != 0:
	 tags.update({'contour':'elevation'})

    if int(height) != 0:
	    if int(height) % 100 == 0:
		tags.update({'contourtype':'100m'})
		tags.update({'name':height})
	    elif int(height) % 50 == 0:
		tags.update({'contourtype':'50m'})
	    elif int(height) % 20 == 0:
		tags.update({'contourtype':'20m'})
	    elif int(height) % 10 == 0:
		tags.update({'contourtype':'10m'})

    return tags