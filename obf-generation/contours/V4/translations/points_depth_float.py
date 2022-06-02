def filterTags(attrs):
    if not attrs:
        return
    tags = {}

    tags.update({'point':'depth'})
    tags.update({'name':str(round(float(attrs['rast_val']), 1))})

    return tags