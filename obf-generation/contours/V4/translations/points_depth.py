def filterTags(attrs):
    if not attrs:
        return
    tags = {}

    depth = attrs['depth'].split('.')[0]
#     if 'depth' in attrs:
#         tags['depth'] = depth

    if int(depth) != 0:
	 tags.update({'point':'depth'})
	 tags.update({'name':depth})

    return tags