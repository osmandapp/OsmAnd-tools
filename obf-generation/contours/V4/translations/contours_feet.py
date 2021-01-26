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
		if int(height) % 1000 == 0:
			tags.update({'contourtype':'1000f'})
		elif int(height) % 200 == 0:
			tags.update({'contourtype':'200f'})
		elif int(height) % 40 == 0:
			tags.update({'contourtype':'40f'})
	return tags