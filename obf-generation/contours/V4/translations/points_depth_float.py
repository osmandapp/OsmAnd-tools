def filterTags(attrs):
    if not attrs:
        return
    tags = {}

    tags.update({'point':'depth'})
    tags.update({'name':attrs['depth']})

    return tags