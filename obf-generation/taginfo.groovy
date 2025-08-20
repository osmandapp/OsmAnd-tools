def processType(tp, uniqueset, tags) {
    def tg = tp.@tag.toString()
    def value = tp.@value.toString()

    if (uniqueset[tg + "=" + value]) {
        return
    }
    uniqueset[tg + "=" + value] = tg

    boolean skipTag = (
        tg.contains("osmand") || 
        tp.@"no_edit" == "true" || 
        tp.@"seq".toString().length() == 0 ||
        tp.@"hidden" == "true" ||
        tp.@"notosm" == "true"  
    )
    if (skipTag) {
        return
    }

    if (value != "") {
        def taginfop = [:]
        taginfop["key"] = tg
        taginfop["value"] = value
        taginfop["description"] = "Used to create maps"
        tags << taginfop
    }
}

def processEntityConvert(tp, uniqueset, tags) {
    def tg = tp.@"from_tag".toString()
    def value = tp.@"from_value".toString()
    if (uniqueset[tg + "=" + value]) {
        return
    }
    uniqueset[tg + "=" + value] = tg

    boolean skipTag = (
        tg.contains("osmand") || 
        tp.@"seq".toString().length() == 0 ||
        tp.@"hidden" == "true" ||
        tp.@"notosm" == "true"  
    )
    if (skipTag) {
        return
    }

    def taginfop = [:]
    taginfop["key"] = tg
    if (value != "") {
        taginfop["value"] = value
    }
    taginfop["description"] = "Used to create maps"
    tags << taginfop
}

DEFAULT_HTTP_URL = "https://raw.githubusercontent.com/osmandapp/OsmAnd-resources/master/rendering_styles/style-icons/drawable-hdpi/"; 

def processPOItype(tp, uniqueset, tags) {
    def mainTag = tp.@edit_tag?.toString() ?: tp.@tag.toString()
    def mainValue = tp.@edit_value?.toString() ?: tp.@value.toString()
    def name = tp.@name.toString()

    def altTag = tp.@edit_tag2?.toString()
    def altValue = tp.@edit_value2?.toString()

    processTag(mainTag, mainValue, name, tp, uniqueset, tags)
    
    if (altTag && altValue) {
        processTag(altTag, altValue, name, tp, uniqueset, tags)
    }
}

def processTag(tag, value, name, tp, uniqueset, tags) {
    if (uniqueset[tag + "=" + value]) {
        return
    }
    uniqueset[tag + "=" + value] = tag

    boolean skipTag = (
        tag.contains("osmand") || 
        tp.@"no_edit" == "true" || 
        tp.@"seq".toString().length() == 0 ||
        tp.@"hidden" == "true" ||
        tp.@"notosm" == "true"  
    )
    if (skipTag) {
        return
    }

    def taginfop = [:]
    taginfop["key"] = tag
    if (value != "") {
        taginfop["value"] = value
    }
    taginfop["description"] = "Used to create maps (POI)"

    String folder = "resources/rendering_styles/style-icons/drawable-hdpi/"
    def originalName = tp.@"name".toString()
    def originalTag = tp.@"tag".toString()
    def originalValue = tp.@"value".toString()
    
    if (new File(folder, "mx_" + originalName + ".png").exists()) {
        taginfop["icon_url"] = DEFAULT_HTTP_URL + "mx_" + originalName + ".png"
    } else if (new File(folder, "mx_" + originalTag + "_" + originalValue + ".png").exists()) {
        taginfop["icon_url"] = DEFAULT_HTTP_URL + "mx_" + originalTag + "_" + originalValue + ".png"
    } else if (new File(folder, "mx_" + originalValue + ".png").exists()) {
        taginfop["icon_url"] = DEFAULT_HTTP_URL + "mx_" + originalValue + ".png"
    }

    tags << taginfop
}

def processPOIGroup(group, uniqueset, tags) {
	group.poi_type.each {
		tp -> processPOItype(tp, uniqueset, tags)
		processPOIGroup(tp, uniqueset, tags)
	}
	group.poi_additional.each {
		pa -> processPOItype(pa, uniqueset, tags)
	}
	group.poi_category.each {
		pac -> processPOIGroup(pac, uniqueset, tags)
	}
	group.poi_filter.each {
		pf -> processPOIGroup(pf, uniqueset, tags)
	}
	group.poi_additional_category.each {
		pac -> processPOIGroup(pac, uniqueset, tags)
	}
}

def builder = new groovy.json.JsonBuilder()

def json = [:]
json["data_format"] = 1; 
json["data_url"] = "https://builder.osmand.net/taginfo.json"
json["data_updated"] = new Date().format("yyyyMMdd'T'HHmmss'Z'")
json["project"] = [
	"name": "OsmAnd",
	"description": "OsmAnd Maps & Navigation",
	"project_url": "http://osmand.net",
	"icon_url": "https://raw.githubusercontent.com/osmandapp/OsmAnd-misc/master/logo/simple/osmand-app-72.png",
	"contact_name": "OsmAnd Team",
	"contact_email": "contactus@osmand.net"
]; 
def tags = []

def renderingTypes = new XmlSlurper().parse("resources/obf_creation/rendering_types.xml")
def poiTypes = new XmlSlurper().parse("resources/poi/poi_types.xml")
def uniqueset = [:]

renderingTypes.type.each {
	tp -> processType(tp, uniqueset, tags)
}
renderingTypes.routing_type.each {
	tp -> processType(tp, uniqueset, tags)
}

renderingTypes.entity_convert.each {
	tp -> processEntityConvert(tp, uniqueset, tags)
}
renderingTypes.category.each {
	c -> c.type.each {
		tp -> processType(tp, uniqueset, tags)
	}
	c.entity_convert.each {
		tp -> processEntityConvert(tp, uniqueset, tags)
	}
}

processPOIGroup(poiTypes, uniqueset, tags); 

json["tags"] = tags
def txt = groovy.json.JsonOutput.prettyPrint(groovy.json.JsonOutput.toJson(json)); 
println txt
new File("taginfo.json").text = txt
